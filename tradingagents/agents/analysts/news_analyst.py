from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import time
import json
from datetime import datetime

#Import a Unified Log System and Analysis Module Log Decorator
from tradingagents.utils.logging_init import get_logger
from tradingagents.utils.tool_logging import log_analyst_module
#Import Unified News Tool
from tradingagents.tools.unified_news_tool import create_unified_news_tool
#Import Stock Tool Class
from tradingagents.utils.stock_utils import StockUtils
#Import Google Tool Call Processing Device
from tradingagents.agents.utils.google_tool_handler import GoogleToolCallHandler

logger = get_logger("analysts.news")


def create_news_analyst(llm, toolkit):
    @log_analyst_module("news")
    def news_analyst_node(state):
        start_time = datetime.now()

        #🔧 Tool Call counter - to prevent infinite circulation
        tool_call_count = state.get("news_tool_call_count", 0)
        max_tool_calls = 3  #Maximum tool call times
        logger.info(f"The number of calls for the current tool:{tool_call_count}/{max_tool_calls}")

        current_date = state["trade_date"]
        ticker = state["company_of_interest"]

        logger.info(f"[news analyst] Start analysis.{ticker}News, date of transaction:{current_date}")
        session_id = state.get("session_id", "未知会话")
        logger.info(f"[Press Analyst ] Session ID:{session_id}, start time:{start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        #Access to market information
        market_info = StockUtils.get_market_info(ticker)
        logger.info(f"[news analyst] Stock type:{market_info['market_name']}")
        
        #Get company names
        def _get_company_name(ticker: str, market_info: dict) -> str:
            """Get company names by stock code"""
            try:
                if market_info['is_china']:
                    #China Unit A: Access to stock information using a unified interface
                    from tradingagents.dataflows.interface import get_china_stock_info_unified
                    stock_info = get_china_stock_info_unified(ticker)
                    
                    #Parsing stock name
                    if "股票名称:" in stock_info:
                        company_name = stock_info.split("股票名称:")[1].split("\n")[0].strip()
                        logger.debug(f"📊 [DBUG] Gets the Chinese stock name from the unified interface:{ticker} -> {company_name}")
                        return company_name
                    else:
                        logger.warning(f"⚠️ [DEBUG] cannot decipher stock names from the unified interface:{ticker}")
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
        
        company_name = _get_company_name(ticker, market_info)
        logger.info(f"[news analyst] Company name:{company_name}")
        
        #🔧Use a unified public information tool to simplify its use
        logger.info(f"[news analyst] Use a unified news tool to automatically identify stock types and access corresponding news")
   #Create a unified public information tool
        unified_news_tool = create_unified_news_tool(toolkit)
        unified_news_tool.name = "get_stock_news_unified"
        
        tools = [unified_news_tool]
        logger.info(f"[news analyst] A unified news tool has been loaded: get stock news unified")

        system_message = (
            """您是一位专业的财经新闻分析师，负责分析最新的市场新闻和事件对股票价格的潜在影响。

您的主要职责包括：
1. 获取和分析最新的实时新闻（优先15-30分钟内的新闻）
2. 评估新闻事件的紧急程度和市场影响
3. 识别可能影响股价的关键信息
4. 分析新闻的时效性和可靠性
5. 提供基于新闻的交易建议和价格影响评估

重点关注的新闻类型：
- 财报发布和业绩指导
- 重大合作和并购消息
- 政策变化和监管动态
- 突发事件和危机管理
- 行业趋势和技术突破
- 管理层变动和战略调整

分析要点：
- 新闻的时效性（发布时间距离现在多久）
- 新闻的可信度（来源权威性）
- 市场影响程度（对股价的潜在影响）
- 投资者情绪变化（正面/负面/中性）
- 与历史类似事件的对比

📊 新闻影响分析要求：
- 评估新闻对股价的短期影响（1-3天）和市场情绪变化
- 分析新闻的利好/利空程度和可能的市场反应
- 评估新闻对公司基本面和长期投资价值的影响
- 识别新闻中的关键信息点和潜在风险
- 对比历史类似事件的市场反应
- 不允许回复'无法评估影响'或'需要更多信息'

请特别注意：
⚠️ 如果新闻数据存在滞后（超过2小时），请在分析中明确说明时效性限制
✅ 优先分析最新的、高相关性的新闻事件
📊 提供新闻对市场情绪和投资者信心的影响评估
💰 必须包含基于新闻的市场反应预期和投资建议
🎯 聚焦新闻内容本身的解读，不涉及技术指标分析

请撰写详细的中文分析报告，并在报告末尾附上Markdown表格总结关键发现。"""
        )

        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "您是一位专业的财经新闻分析师。"
                    "\n🚨 CRITICAL REQUIREMENT - 绝对强制要求："
                    "\n"
                    "\n❌ 禁止行为："
                    "\n- 绝对禁止在没有调用工具的情况下直接回答"
                    "\n- 绝对禁止基于推测或假设生成任何分析内容"
                    "\n- 绝对禁止跳过工具调用步骤"
                    "\n- 绝对禁止说'我无法获取实时数据'等借口"
                    "\n"
                    "\n✅ 强制执行步骤："
                    "\n1. 您的第一个动作必须是调用 get_stock_news_unified 工具"
                    "\n2. 该工具会自动识别股票类型（A股、港股、美股）并获取相应新闻"
                    "\n3. 只有在成功获取新闻数据后，才能开始分析"
                    "\n4. 您的回答必须基于工具返回的真实数据"
                    "\n"
                    "\n🔧 工具调用格式示例："
                    "\n调用: get_stock_news_unified(stock_code='{ticker}', max_news=10)"
                    "\n"
                    "\n⚠️ 如果您不调用工具，您的回答将被视为无效并被拒绝。"
                    "\n⚠️ 您必须先调用工具获取数据，然后基于数据进行分析。"
                    "\n⚠️ 没有例外，没有借口，必须调用工具。"
                    "\n"
                    "\n您可以访问以下工具：{tool_names}。"
                    "\n{system_message}"
                    "\n供您参考，当前日期是{current_date}。我们正在查看公司{ticker}。"
                    "\n请按照上述要求执行，用中文撰写所有分析内容。",
                ),
                MessagesPlaceholder(variable_name="messages"),
            ]
        )

        prompt = prompt.partial(system_message=system_message)
        prompt = prompt.partial(tool_names=", ".join([tool.name for tool in tools]))
        prompt = prompt.partial(current_date=current_date)
        prompt = prompt.partial(ticker=ticker)
        
        #Access to model information for special processing of unified information tools
        model_info = ""
        try:
            if hasattr(llm, 'model_name'):
                model_info = f"{llm.__class__.__name__}:{llm.model_name}"
            else:
                model_info = llm.__class__.__name__
        except:
            model_info = "Unknown"
        
        logger.info(f"[Press Analyst] Ready to call LLM for news analysis. Model:{model_info}")
        
        #🚨 DashScop/DeepSeek/Zhipu pre-processing: mandatory access to news data
        pre_fetched_news = None
        if ('DashScope' in llm.__class__.__name__ 
            or 'DeepSeek' in llm.__class__.__name__
            or 'Zhipu' in llm.__class__.__name__
            ):
            logger.warning(f"[news analyst] 🚨 detected{llm.__class__.__name__}Model, pre-process mandatory news acquisition...")
            try:
                #Forced advance access to news data
                logger.info(f"[news analyst] 🔧 Pre-processing: mandatory call for unified news tools...")
                logger.info(f"[news analyst] 📊 Call parameters: stock code={ticker}, max_news=10, model_info={model_info}")

                pre_fetched_news = unified_news_tool(stock_code=ticker, max_news=10, model_info=model_info)

                logger.info(f"[news analyst] 📋 Length of pre-processed returns:{len(pre_fetched_news) if pre_fetched_news else 0}Character")
                logger.info(f"[Press Analyst] 📄 Pre-processed preview of return results (front 500 characters):{pre_fetched_news[:500] if pre_fetched_news else 'None'}")

                if pre_fetched_news and len(pre_fetched_news.strip()) > 100:
                    logger.info(f"[Press Analyst] ✅ Pre-processed access to news:{len(pre_fetched_news)}Character")

                    #Directly based on pre-accessed news generation analysis, skipping tool call
                    #🔧 Important: Build a system alert that does not contain tools to call guidance
                    analysis_system_prompt = f"""您是一位专业的财经新闻分析师。

您的职责是基于提供的新闻数据，对股票进行深入的新闻分析。

分析要点：
1. 总结最新的新闻事件和市场动态
2. 分析新闻对股票的潜在影响
3. 评估市场情绪和投资者反应
4. 提供基于新闻的投资建议

重要说明：新闻数据已经为您提供，您无需调用任何工具，直接基于提供的数据进行分析。"""

                    enhanced_prompt = f"""请基于以下已获取的最新新闻数据，对股票 {ticker}（{company_name}）进行详细的新闻分析：

=== 最新新闻数据 ===
{pre_fetched_news}

请撰写详细的中文分析报告，包括：
1. 新闻事件总结
2. 对股票的影响分析
3. 市场情绪评估
4. 投资建议"""

                    logger.info(f"[news analyst] 🔄 Use pre-accessed news data to generate analysis directly...")
                    logger.info(f"[news analyst] 📝 System alert length:{len(analysis_system_prompt)}Character")
                    logger.info(f"[Press analyst] 📝 User hint length:{len(enhanced_prompt)}Character")

                    llm_start_time = datetime.now()
                    #🔧 Important: Passing system messages and user messages without tool calls
                    result = llm.invoke([
                        {"role": "system", "content": analysis_system_prompt},
                        {"role": "user", "content": enhanced_prompt}
                    ])

                    llm_end_time = datetime.now()
                    llm_time_taken = (llm_end_time - llm_start_time).total_seconds()
                    logger.info(f"[News Analyst] LLM call completed (pre-processing mode), time-consuming:{llm_time_taken:.2f}sec")

                    #Go straight back to the results, skip the follow-up tool call check
                    if hasattr(result, 'content') and result.content:
                        report = result.content
                        logger.info(f"[Press analyst] ✅ Pre-treatment model successfully, report length:{len(report)}Character")
                        logger.info(f"[news analyst] 📄 report preview (prefix 300 characters):{report[:300]}")

                        #Jump to Final Process
                        from langchain_core.messages import AIMessage
                        clean_message = AIMessage(content=report)

                        end_time = datetime.now()
                        time_taken = (end_time - start_time).total_seconds()
                        logger.info(f"[news analyst] Public information analysis completed (pre-processing mode), total time-consuming:{time_taken:.2f}sec")
                        #Update tool call counters
                        return {
                            "messages": [clean_message],
                            "news_report": report,
                            "news_tool_call_count": tool_call_count + 1
                        }
                    else:
                        logger.warning(f"[Press Analyst] ⚠️ LLM returns empty, back to standard mode")

                else:
                    logger.warning(f"[Press Analyst] ⚠️ Pre-processed access to news failed or was too short ({len(pre_fetched_news) if pre_fetched_news else 0}Character) Back to Standard Mode")
                    if pre_fetched_news:
                        logger.warning(f"[Press Analyst] Failed news content:{pre_fetched_news}")

            except Exception as e:
                logger.error(f"[Press analyst] Pre-treatment failed:{e}Back to standard mode")
                import traceback
                logger.error(f"[news analyst] 📋 Anomalous stack:{traceback.format_exc()}")
        
        #Use a single Google tool to call for processing Device
        llm_start_time = datetime.now()
        chain = prompt | llm.bind_tools(tools)
        logger.info(f"[Press Analyst ] Start the LLM call, analyze.{ticker}Public information")
        #Fix: pass the dictionary instead of the direct message list so that ChatPromptTemplate can handle all variables correctly
        result = chain.invoke({"messages": state["messages"]})
        
        llm_end_time = datetime.now()
        llm_time_taken = (llm_end_time - llm_start_time).total_seconds()
        logger.info(f"[Press Analyst] LLM call completed, time-consuming:{llm_time_taken:.2f}sec")

        #Use a single Google tool to call for processing Device
        if GoogleToolCallHandler.is_google_model(llm):
            logger.info(f"[news analyst] Device")
            
            #Create Analytic Tips
            analysis_prompt_template = GoogleToolCallHandler.create_analysis_prompt(
                ticker=ticker,
                company_name=company_name,
                analyst_type="新闻分析",
                specific_requirements="重点关注新闻事件对股价的影响、市场情绪变化、政策影响等。"
            )
            
            #Process Google Model Tool Call
            report, messages = GoogleToolCallHandler.handle_google_tool_calls(
                result=result,
                llm=llm,
                tools=tools,
                state=state,
                analysis_prompt_template=analysis_prompt_template,
                analyst_name="新闻分析师"
            )
        else:
            #Non-Google processing logic
            logger.info(f"[news analyst] Non-Google model ({llm.__class__.__name__}) using standard processing logic")

            #Check tool calls
            current_tool_calls = len(result.tool_calls) if hasattr(result, 'tool_calls') else 0
            logger.info(f"[Press Analyst] LLM called.{current_tool_calls}A tool")
            logger.debug(f"[DBUG] Cumulative tool call times:{tool_call_count}/{max_tool_calls}")

            if current_tool_calls == 0:
                logger.warning(f"[Press Analyst ]{llm.__class__.__name__}There's no tool to activate the remediation mechanism...")
                logger.warning(f"[Press Analyst] 📄LLLM original response content (front 500 characters):{result.content[:500] if hasattr(result, 'content') else 'No content'}")

                try:
                    #Mandatory access to news data
                    logger.info(f"[Press Analyst] 🔧 Forced access to unified news tools for news data...")
                    logger.info(f"[news analyst] 📊 Call parameters: stock code={ticker}, max_news=10")

                    forced_news = unified_news_tool(stock_code=ticker, max_news=10, model_info=model_info)

                    logger.info(f"[news analyst] 📋 Forced access to return length:{len(forced_news) if forced_news else 0}Character")
                    logger.info(f"[Press Analyst] 📄 Forced to get a preview of the return results (front 500 characters):{forced_news[:500] if forced_news else 'None'}")

                    if forced_news and len(forced_news.strip()) > 100:
                        logger.info(f"[Press Analyst] ✅ Forced access to news success:{len(forced_news)}Character")

                        #Regeneration analysis based on real news data
                        forced_prompt = f"""
您是一位专业的财经新闻分析师。请基于以下最新获取的新闻数据，对股票 {ticker}（{company_name}）进行详细的新闻分析：

=== 最新新闻数据 ===
{forced_news}

=== 分析要求 ===
{system_message}

请基于上述真实新闻数据撰写详细的中文分析报告。
"""

                        logger.info(f"[Press Analyst] 🔄 Regenerated full analysis based on mandatory access to news data...")
                        logger.info(f"[Press Analyst] 📝 Forced reminder length:{len(forced_prompt)}Character")

                        forced_result = llm.invoke([{"role": "user", "content": forced_prompt}])

                        if hasattr(forced_result, 'content') and forced_result.content:
                            report = forced_result.content
                            logger.info(f"[news analyst] ✅ Forced remediation success, generating reports based on real data, length:{len(report)}Character")
                            logger.info(f"[news analyst] 📄 report preview (prefix 300 characters):{report[:300]}")
                        else:
                            logger.warning(f"[news analyst] ⚠️ Forced remediation LLM back empty, using original results")
                            report = result.content if hasattr(result, 'content') else ""
                    else:
                        logger.warning(f"[news analyst] ⚠️ Unified news tool failed or was too short ({len(forced_news) if forced_news else 0}Characters), use original results")
                        if forced_news:
                            logger.warning(f"[Press Analyst] Failed news content:{forced_news}")
                        report = result.content if hasattr(result, 'content') else ""

                except Exception as e:
                    logger.error(f"[news analyst] ❌ Forced remedial process failed:{e}")
                    import traceback
                    logger.error(f"[news analyst] 📋 Anomalous stack:{traceback.format_exc()}")
                    report = result.content if hasattr(result, 'content') else ""
            else:
                #Tools to call, direct results
                report = result.content
        
        total_time_taken = (datetime.now() - start_time).total_seconds()
        logger.info(f"[Press Analyst] News analysis completed, total time-consuming:{total_time_taken:.2f}sec")

        #🔧 Retrieving the loop: returning to clean AIMESSAGE, excluding tool calls
        #This ensures that work flow maps are correctly judged and analysed, avoiding duplication of calls
        from langchain_core.messages import AIMessage
        clean_message = AIMessage(content=report)

        logger.info(f"[news analyst] ✅ returns the cleaning message, report length:{len(report)}Character")

        #Update tool call counters
        return {
            "messages": [clean_message],
            "news_report": report,
            "news_tool_call_count": tool_call_count + 1
        }

    return news_analyst_node
