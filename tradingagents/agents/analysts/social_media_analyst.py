from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import time
import json

#Import a Unified Log System and Analysis Module Log Decorator
from tradingagents.utils.logging_init import get_logger
from tradingagents.utils.tool_logging import log_analyst_module
logger = get_logger("analysts.social_media")

#Import Google Tool Call Processing Device
from tradingagents.agents.utils.google_tool_handler import GoogleToolCallHandler


def _get_company_name_for_social_media(ticker: str, market_info: dict) -> str:
    """Get company names for social media analysts

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

            logger.debug(f"[Social Media Analyst ]{stock_info[:200] if stock_info else 'None'}...")

            #Parsing stock name
            if stock_info and "股票名称:" in stock_info:
                company_name = stock_info.split("股票名称:")[1].split("\n")[0].strip()
                logger.info(f"[Social Media Analyst ]{ticker} -> {company_name}")
                return company_name
            else:
                #Downscaling: attempt to obtain directly from the data source manager
                logger.warning(f"[Social Media Analyst]{ticker}, try to downgrade")
                try:
                    from tradingagents.dataflows.data_source_manager import get_china_stock_info_unified as get_info_dict
                    info_dict = get_info_dict(ticker)
                    if info_dict and info_dict.get('name'):
                        company_name = info_dict['name']
                        logger.info(f"✅ [Social Media Analyst] The downgrading program succeeded in obtaining stock names:{ticker} -> {company_name}")
                        return company_name
                except Exception as e:
                    logger.error(f"The downgrading programme also failed:{e}")

                logger.error(f"[Social Media Analyst] None of the programs can get stock names:{ticker}")
                return f"股票代码{ticker}"

        elif market_info['is_hk']:
            #Port Unit: use of improved Port Unit tools
            try:
                from tradingagents.dataflows.providers.hk.improved_hk import get_hk_company_name_improved
                company_name = get_hk_company_name_improved(ticker)
                logger.debug(f"[Social Media Analyst]{ticker} -> {company_name}")
                return company_name
            except Exception as e:
                logger.debug(f"📊 [Social Media Analyst] failed to improve the Hong Kong Unit tool to get a name:{e}")
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
            logger.debug(f"[Social Media Analyst ] America name map:{ticker} -> {company_name}")
            return company_name

        else:
            return f"股票{ticker}"

    except Exception as e:
        logger.error(f"[Social Media Analyst ]{e}")
        return f"股票{ticker}"


def create_social_media_analyst(llm, toolkit):
    @log_analyst_module("social_media")
    def social_media_analyst_node(state):
        #🔧 Tool Call counter - to prevent infinite circulation
        tool_call_count = state.get("sentiment_tool_call_count", 0)
        max_tool_calls = 3  #Maximum tool call times
        logger.info(f"The number of calls for the current tool:{tool_call_count}/{max_tool_calls}")

        current_date = state["trade_date"]
        ticker = state["company_of_interest"]

        #Access to stock market information
        from tradingagents.utils.stock_utils import StockUtils
        market_info = StockUtils.get_market_info(ticker)

        #Get company names
        company_name = _get_company_name_for_social_media(ticker, market_info)
        logger.info(f"[Social Media Analyst] Company name:{company_name}")

        #Get stock sentation unified tool
        #The tool automatically identifies stock types and calls emotional data. Source
        logger.info(f"[Social Media Analyst] Automatically recognize stock types using unified emotional analysis tools")
        tools = [toolkit.get_stock_sentiment_unified]

        system_message = (
            """您是一位专业的中国市场社交媒体和投资情绪分析师，负责分析中国投资者对特定股票的讨论和情绪变化。

您的主要职责包括：
1. 分析中国主要财经平台的投资者情绪（如雪球、东方财富股吧等）
2. 监控财经媒体和新闻对股票的报道倾向
3. 识别影响股价的热点事件和市场传言
4. 评估散户与机构投资者的观点差异
5. 分析政策变化对投资者情绪的影响
6. 评估情绪变化对股价的潜在影响

重点关注平台：
- 财经新闻：财联社、新浪财经、东方财富、腾讯财经
- 投资社区：雪球、东方财富股吧、同花顺
- 社交媒体：微博财经大V、知乎投资话题
- 专业分析：各大券商研报、财经自媒体

分析要点：
- 投资者情绪的变化趋势和原因
- 关键意见领袖(KOL)的观点和影响力
- 热点事件对股价预期的影响
- 政策解读和市场预期变化
- 散户情绪与机构观点的差异

📊 情绪影响分析要求：
- 量化投资者情绪强度（乐观/悲观程度）和情绪变化趋势
- 评估情绪变化对短期市场反应的影响（1-5天）
- 分析散户情绪与市场走势的相关性
- 识别情绪极端点和可能的情绪反转信号
- 提供基于情绪分析的市场预期和投资建议
- 评估市场情绪对投资者信心和决策的影响程度
- 不允许回复'无法评估情绪影响'或'需要更多数据'

💰 必须包含：
- 情绪指数评分（1-10分）
- 预期价格波动幅度
- 基于情绪的交易时机建议

请撰写详细的中文分析报告，并在报告末尾附上Markdown表格总结关键发现。
注意：由于中国社交媒体API限制，如果数据获取受限，请明确说明并提供替代分析建议。"""
        )

        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "您是一位有用的AI助手，与其他助手协作。"
                    " 使用提供的工具来推进回答问题。"
                    " 如果您无法完全回答，没关系；具有不同工具的其他助手"
                    " 将从您停下的地方继续帮助。执行您能做的以取得进展。"
                    " 如果您或任何其他助手有最终交易提案：**买入/持有/卖出**或可交付成果，"
                    " 请在您的回应前加上最终交易提案：**买入/持有/卖出**，以便团队知道停止。"
                    " 您可以访问以下工具：{tool_names}。\n{system_message}"
                    "供您参考，当前日期是{current_date}。我们要分析的当前公司是{ticker}。请用中文撰写所有分析内容。",
                ),
                MessagesPlaceholder(variable_name="messages"),
            ]
        )

        prompt = prompt.partial(system_message=system_message)
        #Securely capture tool names, process functions and tool objects
        tool_names = []
        for tool in tools:
            if hasattr(tool, 'name'):
                tool_names.append(tool.name)
            elif hasattr(tool, '__name__'):
                tool_names.append(tool.__name__)
            else:
                tool_names.append(str(tool))

        prompt = prompt.partial(tool_names=", ".join(tool_names))
        prompt = prompt.partial(current_date=current_date)
        prompt = prompt.partial(ticker=ticker)

        chain = prompt | llm.bind_tools(tools)

        #Fix: pass the dictionary instead of the direct message list so that ChatPromptTemplate can handle all variables correctly
        result = chain.invoke({"messages": state["messages"]})

        #Use a single Google tool to call for processing Device
        if GoogleToolCallHandler.is_google_model(llm):
            logger.info(f"[Social Media Analyst] Device")
            
            #Create Analytic Tips
            analysis_prompt_template = GoogleToolCallHandler.create_analysis_prompt(
                ticker=ticker,
                company_name=company_name,
                analyst_type="社交媒体情绪分析",
                specific_requirements="重点关注投资者情绪、社交媒体讨论热度、舆论影响等。"
            )
            
            #Process Google Model Tool Call
            report, messages = GoogleToolCallHandler.handle_google_tool_calls(
                result=result,
                llm=llm,
                tools=tools,
                state=state,
                analysis_prompt_template=analysis_prompt_template,
                analyst_name="社交媒体分析师"
            )
        else:
            #Non-Google processing logic
            logger.debug(f"[DEBUG] Non-Google model{llm.__class__.__name__}) using standard processing logic")
            
            report = ""
            if len(result.tool_calls) == 0:
                report = result.content

        #Update tool call counters
        return {
            "messages": [result],
            "sentiment_report": report,
            "sentiment_tool_call_count": tool_call_count + 1
        }

    return social_media_analyst_node
