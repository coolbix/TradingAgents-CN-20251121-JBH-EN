from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import time
import json
import traceback

#Import Analysis Module Log Decorator
from tradingagents.utils.tool_logging import log_analyst_module

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("default")

#Import Google Tool Call Processing Device
from tradingagents.agents.utils.google_tool_handler import GoogleToolCallHandler


def _get_company_name(ticker: str, market_info: dict) -> str:
    """Get company names by stock code

Args:
ticker: Stock code
market info: market information dictionary

Returns:
str: Company name
"""
    try:
        if market_info['is_china']:
            #China Unit A: Access to stock information using a unified interface
            from tradingagents.dataflows.interface import get_china_stock_info_unified
            stock_info = get_china_stock_info_unified(ticker)

            logger.debug(f"[Market Analyst ] To get stock information back:{stock_info[:200] if stock_info else 'None'}...")

            #Parsing stock name
            if stock_info and "股票名称:" in stock_info:
                company_name = stock_info.split("股票名称:")[1].split("\n")[0].strip()
                logger.info(f"✅ [Market Analyst] Successfully obtained Chinese stock names:{ticker} -> {company_name}")
                return company_name
            else:
                #Downscaling: attempt to obtain directly from the data source manager
                logger.warning(f"The name of the stock could not be deciphered from the unified interface:{ticker}, try to downgrade")
                try:
                    from tradingagents.dataflows.data_source_manager import get_china_stock_info_unified as get_info_dict
                    info_dict = get_info_dict(ticker)
                    if info_dict and info_dict.get('name'):
                        company_name = info_dict['name']
                        logger.info(f"✅ [Market Analyst] The downgrading program successfully obtained the name of the stock:{ticker} -> {company_name}")
                        return company_name
                except Exception as e:
                    logger.error(f"The downgrading programme also failed:{e}")

                logger.error(f"[Market Analyst] None of the programs can get stock names:{ticker}")
                return f"股票代码{ticker}"

        elif market_info['is_hk']:
            #Port Unit: use of improved Port Unit tools
            try:
                from tradingagents.dataflows.providers.hk.improved_hk import get_hk_company_name_improved
                company_name = get_hk_company_name_improved(ticker)
                logger.debug(f"📊 [DBUG] Use of the Port Improvement Unit tool to obtain names:{ticker} -> {company_name}")
                return company_name
            except Exception as e:
                logger.debug(f"📊 [DBUG] Improvements to the Port Unit Tool to get names failed:{e}")
                #Downscaling scheme: Generate friendly default names
                clean_ticker = ticker.replace('.HK', '').replace('.hk', '')
                return f"港股{clean_ticker}"

        elif market_info['is_us']:
            #US share: use simple mapping or return code
            us_stock_names = {
                'AAPL': '苹果公司',
                'TSLA': '特斯拉',
                'NVDA': '英伟达',
                'MSFT': '微软',
                'GOOGL': '谷歌',
                'AMZN': '亚马逊',
                'META': 'Meta',
                'NFLX': '奈飞'
            }

            company_name = us_stock_names.get(ticker.upper(), f"美股{ticker}")
            logger.debug(f"[DEBUG] U.S. stock name map:{ticker} -> {company_name}")
            return company_name

        else:
            return f"股票{ticker}"

    except Exception as e:
        logger.error(f"[DEBUG]{e}")
        return f"股票{ticker}"


def create_market_analyst(llm, toolkit):

    def market_analyst_node(state):
        logger.debug(f"== sync, corrected by elderman == @elder man")

        #🔧 Tool Call counter - to prevent infinite circulation
        tool_call_count = state.get("market_tool_call_count", 0)
        max_tool_calls = 3  #Maximum tool call times
        logger.info(f"The number of calls for the current tool:{tool_call_count}/{max_tool_calls}")

        current_date = state["trade_date"]
        ticker = state["company_of_interest"]

        logger.debug(f"[DBUG] input parameter: ticker={ticker}, date={current_date}")
        logger.debug(f"The number of messages in the current state:{len(state.get('messages', []))}")
        logger.debug(f"[DBUG] Available market reports:{state.get('market_report', 'None')}")

        #Select the data source according to the stock code format
        from tradingagents.utils.stock_utils import StockUtils

        market_info = StockUtils.get_market_info(ticker)

        logger.debug(f"[DBUG] Stock type checks:{ticker} -> {market_info['market_name']} ({market_info['currency_name']})")

        #Get company names
        company_name = _get_company_name(ticker, market_info)
        logger.debug(f"[DEBUG]{ticker} -> {company_name}")

        #Get stock mark data unified tool
        #The tool automatically identifies stock types (A/Hong Kong/US) and calls the corresponding data. Source
        logger.info(f"📊 [Market Analyst] Automatically identify stock types using the Unified Market Data Tool")
        tools = [toolkit.get_stock_market_data_unified]

        #Securely capture tool names for debugging
        tool_names_debug = []
        for tool in tools:
            if hasattr(tool, 'name'):
                tool_names_debug.append(tool.name)
            elif hasattr(tool, '__name__'):
                tool_names_debug.append(tool.__name__)
            else:
                tool_names_debug.append(str(tool))
        logger.info(f"[Market Analyst]{tool_names_debug}")
        logger.info(f"[Market Analyst] Target market:{market_info['market_name']}")

        #Optimization: Placing the output format requirement at the beginning of the system alert to ensure that the LLM follows the format
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "你是一位专业的股票技术分析师，与其他分析师协作。\n"
                    "\n"
                    "📋 **分析对象：**\n"
                    "- 公司名称：{company_name}\n"
                    "- 股票代码：{ticker}\n"
                    "- 所属市场：{market_name}\n"
                    "- 计价货币：{currency_name}（{currency_symbol}）\n"
                    "- 分析日期：{current_date}\n"
                    "\n"
                    "🔧 **工具使用：**\n"
                    "你可以使用以下工具：{tool_names}\n"
                    "⚠️ 重要工作流程：\n"
                    "1. 如果消息历史中没有工具结果，立即调用 get_stock_market_data_unified 工具\n"
                    "   - ticker: {ticker}\n"
                    "   - start_date: {current_date}\n"
                    "   - end_date: {current_date}\n"
                    "   注意：系统会自动扩展到365天历史数据，你只需要传递当前分析日期即可\n"
                    "2. 如果消息历史中已经有工具结果（ToolMessage），立即基于工具数据生成最终分析报告\n"
                    "3. 不要重复调用工具！一次工具调用就足够了！\n"
                    "4. 接收到工具数据后，必须立即生成完整的技术分析报告，不要再调用任何工具\n"
                    "\n"
                    "📝 **输出格式要求（必须严格遵守）：**\n"
                    "\n"
                    "## 📊 股票基本信息\n"
                    "- 公司名称：{company_name}\n"
                    "- 股票代码：{ticker}\n"
                    "- 所属市场：{market_name}\n"
                    "\n"
                    "## 📈 技术指标分析\n"
                    "[在这里分析移动平均线、MACD、RSI、布林带等技术指标，提供具体数值]\n"
                    "\n"
                    "## 📉 价格趋势分析\n"
                    "[在这里分析价格趋势，考虑{market_name}市场特点]\n"
                    "\n"
                    "## 💭 投资建议\n"
                    "[在这里给出明确的投资建议：买入/持有/卖出]\n"
                    "\n"
                    "⚠️ **重要提醒：**\n"
                    "- 必须使用上述格式输出，不要自创标题格式\n"
                    "- 所有价格数据使用{currency_name}（{currency_symbol}）表示\n"
                    "- 确保在分析中正确使用公司名称\"{company_name}\"和股票代码\"{ticker}\"\n"
                    "- 不要在标题中使用\"技术分析报告\"等自创标题\n"
                    "- 如果你有明确的技术面投资建议（买入/持有/卖出），请在投资建议部分明确标注\n"
                    "- 不要使用'最终交易建议'前缀，因为最终决策需要综合所有分析师的意见\n"
                    "\n"
                    "请使用中文，基于真实数据进行分析。",
                ),
                MessagesPlaceholder(variable_name="messages"),
            ]
        )

        #Securely capture tool names, process functions and tool objects
        tool_names = []
        for tool in tools:
            if hasattr(tool, 'name'):
                tool_names.append(tool.name)
            elif hasattr(tool, '__name__'):
                tool_names.append(tool.__name__)
            else:
                tool_names.append(str(tool))

        #Set all template variables
        prompt = prompt.partial(tool_names=", ".join(tool_names))
        prompt = prompt.partial(current_date=current_date)
        prompt = prompt.partial(ticker=ticker)
        prompt = prompt.partial(company_name=company_name)
        prompt = prompt.partial(market_name=market_info['market_name'])
        prompt = prompt.partial(currency_name=market_info['currency_name'])
        prompt = prompt.partial(currency_symbol=market_info['currency_symbol'])

        #Add Detailed Log
        logger.info(f"[Market Analyst] LLM type:{llm.__class__.__name__}")
        logger.info(f"[Market Analyst] LLM model:{getattr(llm, 'model_name', 'unknown')}")
        logger.info(f"[Market Analyst]{len(state['messages'])}")
        logger.info(f"[Market Analyst]{company_name}")
        logger.info(f"[Market Analyst ] Stock code:{ticker}")

        #Print hint template information
        logger.info("📊 [Market Analyst] = = = = = = Transcript information = = = = = = = = = = = = = = = = = = = = = = = = =")
        logger.info(f"[Market Analyst] Template variable set: company name={company_name}, ticker={ticker}, market={market_info['market_name']}")
        logger.info("📊 [Market Analyst] ==================================================================================================================================================================================================================================================")

        #Print the actual message to LLM
        logger.info(f"📊 [Market Analyst] = = = = = = = = message to LLM = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =")
        for i, msg in enumerate(state["messages"]):
            msg_type = type(msg).__name__
            #🔥Recovery: extracting messages more securely
            if hasattr(msg, 'content'):
                msg_content = str(msg.content)[:500]  #Increase to 500 characters to see full content
            elif isinstance(msg, tuple) and len(msg) >= 2:
                #Deals with old group messages ( "human", "content")
                msg_content = f"[元组消息] 类型={msg[0]}, 内容={str(msg[1])[:500]}"
            else:
                msg_content = str(msg)[:500]
            logger.info(f"[Market Analyst ]{i}Type ={msg_type}, content={msg_content}")
        logger.info(f"📊 [Market Analyst] = = = = = = = end of message list = = = = = = = = = = = = = = = =")

        chain = prompt | llm.bind_tools(tools)

        logger.info(f"[Market Analyst ]")
        #Fix: pass the dictionary instead of the direct message list so that ChatPromptTemplate can handle all variables correctly
        result = chain.invoke({"messages": state["messages"]})
        logger.info(f"[Market Analyst]")

        #Print LLM response
        logger.info(f"📊 [Market Analyst] = = = = = = LLM response start = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =")
        logger.info(f"[Market Analyst] Type of response:{type(result).__name__}")
        logger.info(f"[Market Analyst ]{str(result.content)[:1000]}...")
        if hasattr(result, 'tool_calls') and result.tool_calls:
            logger.info(f"[Market Analyst]{result.tool_calls}")
        logger.info(f"📊 [Market Analyst] = = = = = = LLM response end= = = = = = = = = = = = = = = = = = = =")

        #Use a single Google tool to call for processing Device
        if GoogleToolCallHandler.is_google_model(llm):
            logger.info(f"[Market Analyst] Device")
            
            #Create Analytic Tips
            analysis_prompt_template = GoogleToolCallHandler.create_analysis_prompt(
                ticker=ticker,
                company_name=company_name,
                analyst_type="市场分析",
                specific_requirements="重点关注市场数据、价格走势、交易量变化等市场指标。"
            )
            
            #Process Google Model Tool Call
            report, messages = GoogleToolCallHandler.handle_google_tool_calls(
                result=result,
                llm=llm,
                tools=tools,
                state=state,
                analysis_prompt_template=analysis_prompt_template,
                analyst_name="市场分析师"
            )

            #Update tool call counters
            return {
                "messages": [result],
                "market_report": report,
                "market_tool_call_count": tool_call_count + 1
            }
        else:
            #Non-Google processing logic
            logger.info(f"[Market Analyst]{llm.__class__.__name__}) using standard processing logic")
            logger.info(f"[Market Analyst ]")
            logger.info(f"[Market Analyst ]{hasattr(result, 'tool_calls')}")
            if hasattr(result, 'tool_calls'):
                logger.info(f"- Tool calls:{len(result.tool_calls)}")
                if result.tool_calls:
                    for i, tc in enumerate(result.tool_calls):
                        logger.info(f"[Market Analyst ]{i}]: {tc.get('name', 'unknown')}")

            #Processing market analysis reports
            if len(result.tool_calls) == 0:
                #No tool to call, directly using LLM responses
                report = result.content
                logger.info(f"Direct response (no tool to call), length:{len(report)}")
                logger.debug(f"📊 [DBUG] directs to the preview:{report[:200]}...")
            else:
                #Tools to call, implement and generate complete analysis
                logger.info(f"[Market Analyst] 📊 has detected a tool call:{[call.get('name', 'unknown') for call in result.tool_calls]}")

                try:
                    #Execute Tool Call
                    from langchain_core.messages import ToolMessage, HumanMessage

                    tool_messages = []
                    for tool_call in result.tool_calls:
                        tool_name = tool_call.get('name')
                        tool_args = tool_call.get('args', {})
                        tool_id = tool_call.get('id')

                        logger.debug(f"[DBUG] Implementation tool:{tool_name}, Parameters:{tool_args}")

                        #Find corresponding tools and execute them
                        tool_result = None
                        for tool in tools:
                            #Comparison of secure access to tool names
                            current_tool_name = None
                            if hasattr(tool, 'name'):
                                current_tool_name = tool.name
                            elif hasattr(tool, '__name__'):
                                current_tool_name = tool.__name__

                            if current_tool_name == tool_name:
                                try:
                                    if tool_name == "get_china_stock_data":
                                        #China Stock Data Tool
                                        tool_result = tool.invoke(tool_args)
                                    else:
                                        #Other tools
                                        tool_result = tool.invoke(tool_args)
                                    logger.debug(f"📊 [DBUG] tool successfully implemented, result length:{len(str(tool_result))}")
                                    break
                                except Exception as tool_error:
                                    logger.error(f"[DEBUG] Tool failed:{tool_error}")
                                    tool_result = f"工具执行失败: {str(tool_error)}"

                        if tool_result is None:
                            tool_result = f"未找到工具: {tool_name}"

                        #Create Tool Message
                        tool_message = ToolMessage(
                            content=str(tool_result),
                            tool_call_id=tool_id
                        )
                        tool_messages.append(tool_message)

                    #Generate complete analysis based on the results of the tool
                    #🔥 Important: This must include company name and output format requirements to ensure that LLM produces the correct report title
                    analysis_prompt = f"""现在请基于上述工具获取的数据，生成详细的技术分析报告。

**分析对象：**
- 公司名称：{company_name}
- 股票代码：{ticker}
- 所属市场：{market_info['market_name']}
- 计价货币：{market_info['currency_name']}（{market_info['currency_symbol']}）

**输出格式要求（必须严格遵守）：**

请按照以下专业格式输出报告，不要使用emoji符号（如📊📈📉💭等），使用纯文本标题：

# **{company_name}（{ticker}）技术分析报告**
**分析日期：[当前日期]**

---

## 一、股票基本信息

- **公司名称**：{company_name}
- **股票代码**：{ticker}
- **所属市场**：{market_info['market_name']}
- **当前价格**：[从工具数据中获取] {market_info['currency_symbol']}
- **涨跌幅**：[从工具数据中获取]
- **成交量**：[从工具数据中获取]

---

## 二、技术指标分析

### 1. 移动平均线（MA）分析

[分析MA5、MA10、MA20、MA60等均线系统，包括：]
- 当前各均线数值
- 均线排列形态（多头/空头）
- 价格与均线的位置关系
- 均线交叉信号

### 2. MACD指标分析

[分析MACD指标，包括：]
- DIF、DEA、MACD柱状图当前数值
- 金叉/死叉信号
- 背离现象
- 趋势强度判断

### 3. RSI相对强弱指标

[分析RSI指标，包括：]
- RSI当前数值
- 超买/超卖区域判断
- 背离信号
- 趋势确认

### 4. 布林带（BOLL）分析

[分析布林带指标，包括：]
- 上轨、中轨、下轨数值
- 价格在布林带中的位置
- 带宽变化趋势
- 突破信号

---

## 三、价格趋势分析

### 1. 短期趋势（5-10个交易日）

[分析短期价格走势，包括支撑位、压力位、关键价格区间]

### 2. 中期趋势（20-60个交易日）

[分析中期价格走势，结合均线系统判断趋势方向]

### 3. 成交量分析

[分析成交量变化，量价配合情况]

---

## 四、投资建议

### 1. 综合评估

[基于上述技术指标，给出综合评估]

### 2. 操作建议

- **投资评级**：买入/持有/卖出
- **目标价位**：[给出具体价格区间] {market_info['currency_symbol']}
- **止损位**：[给出止损价格] {market_info['currency_symbol']}
- **风险提示**：[列出主要风险因素]

### 3. 关键价格区间

- **支撑位**：[具体价格]
- **压力位**：[具体价格]
- **突破买入价**：[具体价格]
- **跌破卖出价**：[具体价格]

---

**重要提醒：**
- 必须严格按照上述格式输出，使用标准的Markdown标题（#、##、###）
- 不要使用emoji符号（📊📈📉💭等）
- 所有价格数据使用{market_info['currency_name']}（{market_info['currency_symbol']}）表示
- 确保在分析中正确使用公司名称"{company_name}"和股票代码"{ticker}"
- 报告标题必须是：# **{company_name}（{ticker}）技术分析报告**
- 报告必须基于工具返回的真实数据进行分析
- 包含具体的技术指标数值和专业分析
- 提供明确的投资建议和风险提示
- 报告长度不少于800字
- 使用中文撰写
- 使用表格展示数据时，确保格式规范"""

                    #Build a complete message sequence
                    messages = state["messages"] + [result] + tool_messages + [HumanMessage(content=analysis_prompt)]

                    #Generate final analysis reports
                    final_result = llm.invoke(messages)
                    report = final_result.content

                    logger.info(f"📊 [market analyst] Generate complete analysis, length:{len(report)}")

                    #Returns complete message sequences containing tool calls and final analysis
                    #Update tool call counters
                    return {
                        "messages": [result] + tool_messages + [final_result],
                        "market_report": report,
                        "market_tool_call_count": tool_call_count + 1
                    }

                except Exception as e:
                    logger.error(f"❌ [market analyst] tool implementation or analysis generation failed:{e}")
                    traceback.print_exc()

                    #Declining: Returning tool call information
                    report = f"市场分析师调用了工具但分析生成失败: {[call.get('name', 'unknown') for call in result.tool_calls]}"

                    #Update tool call counters
                    return {
                        "messages": [result],
                        "market_report": report,
                        "market_tool_call_count": tool_call_count + 1
                    }

            #Update tool call counters
            return {
                "messages": [result],
                "market_report": report,
                "market_tool_call_count": tool_call_count + 1
            }

    return market_analyst_node
