"""Basic Profile Analyst - Unified Tool Architecture Version
Automatically identify stock types and call corresponding data using a uniform tool Source
"""

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import AIMessage, ToolMessage

#Import Analysis Module Log Decorator
from tradingagents.utils.tool_logging import log_analyst_module

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("default")

#Import Google Tool Call Processing Device
from tradingagents.agents.utils.google_tool_handler import GoogleToolCallHandler


def _get_company_name_for_fundamentals(ticker: str, market_info: dict) -> str:
    """Get company names for basic face analysts

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

            logger.debug(f"📊 [BASIC ANALYSISER]{stock_info[:200] if stock_info else 'None'}...")

            #Parsing stock name
            if stock_info and "股票名称:" in stock_info:
                company_name = stock_info.split("股票名称:")[1].split("\n")[0].strip()
                logger.info(f"✅ [Basic Face Analyst] Successfully obtained Chinese stock names:{ticker} -> {company_name}")
                return company_name
            else:
                #Downscaling: attempt to obtain directly from the data source manager
                logger.warning(f"⚠️ [basic profiler] cannot decipher stock names from the unified interface:{ticker}, try to downgrade")
                try:
                    from tradingagents.dataflows.data_source_manager import get_china_stock_info_unified as get_info_dict
                    info_dict = get_info_dict(ticker)
                    if info_dict and info_dict.get('name'):
                        company_name = info_dict['name']
                        logger.info(f"✅ [basic face analyst] The downgrading program successfully obtained the name of the stock:{ticker} -> {company_name}")
                        return company_name
                except Exception as e:
                    logger.error(f"The downgrading programme has also failed:{e}")

                logger.error(f"All options are not available:{ticker}")
                return f"股票代码{ticker}"

        elif market_info['is_hk']:
            #Port Unit: use of improved Port Unit tools
            try:
                from tradingagents.dataflows.providers.hk.improved_hk import get_hk_company_name_improved
                company_name = get_hk_company_name_improved(ticker)
                logger.debug(f"[Basic Profile Analyst]{ticker} -> {company_name}")
                return company_name
            except Exception as e:
                logger.debug(f"📊 [basic analyst] failed to improve the Hong Kong stock tool to get a name:{e}")
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
            logger.debug(f"[BASIC ANALYSISER]{ticker} -> {company_name}")
            return company_name

        else:
            return f"股票{ticker}"

    except Exception as e:
        logger.error(f"❌ [Basic Face Analyst] Failed to get company name:{e}")
        return f"股票{ticker}"


def create_fundamentals_analyst(llm, toolkit):
    @log_analyst_module("fundamentals")
    def fundamentals_analyst_node(state):
        logger.debug(f"[DBUG] = = = beginning of basic surface analyst node")

        #🔧 Tool Call counter - to prevent infinite circulation
        #Check if there is a ToolMessage in the message history, and if so indicate that the tool has been implemented Pass.
        messages = state.get("messages", [])
        tool_message_count = sum(1 for msg in messages if isinstance(msg, ToolMessage))

        tool_call_count = state.get("fundamentals_tool_call_count", 0)
        max_tool_calls = 1  #Maximum tool call times: a tool call will capture all data

        #Update the counter if there is a new ToolMessage
        if tool_message_count > tool_call_count:
            tool_call_count = tool_message_count
            logger.info(f"🔧 [tool call count] new tool results detected, update counters:{tool_call_count}")

        logger.info(f"🔧 [Tool Call Count] Number of times the current tool is called:{tool_call_count}/{max_tool_calls}")

        current_date = state["trade_date"]
        ticker = state["company_of_interest"]

        #🔧 Basic Analysis Data Range: Fixed access to 10 days of data (processing weekends/ holidays/data delays)
        #References: Docs/ANALIST DATA CONFIGURATION.md
        #Basic analysis relies mainly on financial data (PE, PB, ROE, etc.) and requires only current equity prices
        #The data were obtained for 10 days to ensure access, but the actual analysis was only used for the last 2 days
        from datetime import datetime, timedelta
        try:
            end_date_dt = datetime.strptime(current_date, "%Y-%m-%d")
            start_date_dt = end_date_dt - timedelta(days=10)
            start_date = start_date_dt.strftime("%Y-%m-%d")
            logger.info(f"Data range:{start_date}to{current_date}(10 days fixed)")
        except Exception as e:
            #If date resolution failed, use default 10 days ago
            logger.warning(f"Date resolution failed, using default range:{e}")
            start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

        logger.debug(f"[DBUG] input parameter: ticker={ticker}, date={current_date}")
        logger.debug(f"The number of messages in the current state:{len(state.get('messages', []))}")
        logger.debug(f"[DEBUG]{state.get('fundamentals_report', 'None')}")

        #Access to stock market information
        from tradingagents.utils.stock_utils import StockUtils
        logger.info(f"📊 [basic face analyst] is analysing stocks:{ticker}")

        #Add detailed stock code tracking log
        logger.info(f"[Equal code tracking]{ticker}' (type:{type(ticker)})")
        logger.info(f"[Equal code tracking]{len(str(ticker))}")
        logger.info(f"[Equal code tracking]{list(str(ticker))}")

        market_info = StockUtils.get_market_info(ticker)
        logger.info(f"[StockUtils.get market info]{market_info}")

        logger.debug(f"[DBUG] Stock type checks:{ticker} -> {market_info['market_name']} ({market_info['currency_name']}")
        logger.debug(f"[DEBUG]{market_info['is_china']}, is_hk={market_info['is_hk']}, is_us={market_info['is_us']}")
        logger.debug(f"[DBUG] Tool configuration check: online tools={toolkit.config['online_tools']}")

        #Get company names
        company_name = _get_company_name_for_fundamentals(ticker, market_info)
        logger.debug(f"[DEBUG]{ticker} -> {company_name}")

        #Unifiedly use get stock fundamentals unified tools
        #The tool automatically identifies stock types (A/Hong Kong/US) and calls the corresponding data. Source
        #For Unit A, it automatically acquires price and fundamental data without the need for LLM to access multiple tools
        logger.info(f"📊 [Basic Profile Analysts] Automatically recognize stock types using a common fundamentals analysis tool")
        tools = [toolkit.get_stock_fundamentals_unified]

        #Securely capture tool names for debugging
        tool_names_debug = []
        for tool in tools:
            if hasattr(tool, 'name'):
                tool_names_debug.append(tool.name)
            elif hasattr(tool, '__name__'):
                tool_names_debug.append(tool.__name__)
            else:
                tool_names_debug.append(str(tool))
        logger.info(f"📊 [Basic Analyser] Bind tools:{tool_names_debug}")
        logger.info(f"Target market:{market_info['market_name']}")

        #Harmonized system alerts for all stock types
        system_message = (
            f"你是一位专业的股票基本面分析师。"
            f"⚠️ 绝对强制要求：你必须调用工具获取真实数据！不允许任何假设或编造！"
            f"任务：分析{company_name}（股票代码：{ticker}，{market_info['market_name']}）"
            f"🔴 立即调用 get_stock_fundamentals_unified 工具"
            f"参数：ticker='{ticker}', start_date='{start_date}', end_date='{current_date}', curr_date='{current_date}'"
            "📊 分析要求："
            "- 基于真实数据进行深度基本面分析"
            f"- 计算并提供合理价位区间（使用{market_info['currency_name']}{market_info['currency_symbol']}）"
            "- 分析当前股价是否被低估或高估"
            "- 提供基于基本面的目标价位建议"
            "- 包含PE、PB、PEG等估值指标分析"
            "- 结合市场特点进行分析"
            "🌍 语言和货币要求："
            "- 所有分析内容必须使用中文"
            "- 投资建议必须使用中文：买入、持有、卖出"
            "- 绝对不允许使用英文：buy、hold、sell"
            f"- 货币单位使用：{market_info['currency_name']}（{market_info['currency_symbol']}）"
            "🚫 严格禁止："
            "- 不允许说'我将调用工具'"
            "- 不允许假设任何数据"
            "- 不允许编造公司信息"
            "- 不允许直接回答而不调用工具"
            "- 不允许回复'无法确定价位'或'需要更多信息'"
            "- 不允许使用英文投资建议（buy/hold/sell）"
            "✅ 你必须："
            "- 立即调用统一基本面分析工具"
            "- 等待工具返回真实数据"
            "- 基于真实数据进行分析"
            "- 提供具体的价位区间和目标价"
            "- 使用中文投资建议（买入/持有/卖出）"
            "现在立即开始调用工具！不要说任何其他话！"
        )

        #System Tip Template
        system_prompt = (
            "🔴 强制要求：你必须调用工具获取真实数据！"
            "🚫 绝对禁止：不允许假设、编造或直接回答任何问题！"
            "✅ 工作流程："
            "1. 【第一次调用】如果消息历史中没有工具结果（ToolMessage），立即调用 get_stock_fundamentals_unified 工具"
            "2. 【收到数据后】如果消息历史中已经有工具结果（ToolMessage），🚨 绝对禁止再次调用工具！🚨"
            "3. 【生成报告】收到工具数据后，必须立即生成完整的基本面分析报告，包含："
            "   - 公司基本信息和财务数据分析"
            "   - PE、PB、PEG等估值指标分析"
            "   - 当前股价是否被低估或高估的判断"
            "   - 合理价位区间和目标价位建议"
            "   - 基于基本面的投资建议（买入/持有/卖出）"
            "4. 🚨 重要：工具只需调用一次！一次调用返回所有需要的数据！不要重复调用！🚨"
            "5. 🚨 如果你已经看到ToolMessage，说明工具已经返回数据，直接生成报告，不要再调用工具！🚨"
            "可用工具：{tool_names}。\n{system_message}"
            "当前日期：{current_date}。"
            "分析目标：{company_name}（股票代码：{ticker}）。"
            "请确保在分析中正确区分公司名称和股票代码。"
        )

        #Create hint template
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="messages"),
        ])

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
        prompt = prompt.partial(company_name=company_name)

        #Test the Alibra model and create new examples
        if hasattr(llm, '__class__') and 'DashScope' in llm.__class__.__name__:
            logger.debug(f"[DEBUG] detected the Aliblanc model and created new examples to avoid tool caches")
            from tradingagents.llm_adapters import ChatDashScopeOpenAI

            #Fetch original LLM base url and api key
            original_base_url = getattr(llm, 'openai_api_base', None)
            original_api_key = getattr(llm, 'openai_api_key', None)

            fresh_llm = ChatDashScopeOpenAI(
                model=llm.model_name,
                api_key=original_api_key,  #Passing the original LLM API Key
                base_url=original_base_url if original_base_url else None,  #Pass
                temperature=llm.temperature,
                max_tokens=getattr(llm, 'max_tokens', 2000)
            )

            if original_base_url:
                logger.debug(f"[DEBUG] Use original case url:{original_base_url}")
            if original_api_key:
                logger.debug(f"[DBUG] Use original API Key (from database configuration)")
        else:
            fresh_llm = llm

        logger.debug(f"[DBUG] Create LLM chains, number of tools:{len(tools)}")
        #Securely capture tool names for debugging
        debug_tool_names = []
        for tool in tools:
            if hasattr(tool, 'name'):
                debug_tool_names.append(tool.name)
            elif hasattr(tool, '__name__'):
                debug_tool_names.append(tool.__name__)
            else:
                debug_tool_names.append(str(tool))
        logger.debug(f"List of binding tools:{debug_tool_names}")
        logger.debug(f"📊 [DEBUG] Create a tool chain that allows models to decide whether to call the tool or not")

        #Add Detailed Log
        logger.info(f"📊 [basic profiler] LLM type:{fresh_llm.__class__.__name__}")
        logger.info(f"The LLM model:{getattr(fresh_llm, 'model_name', 'unknown')}")
        logger.info(f"📊 [Basic Profile Analyst]{len(state['messages'])}")

        try:
            chain = prompt | fresh_llm.bind_tools(tools)
            logger.info(f"The tool was successfully bound.{len(tools)}A tool")
        except Exception as e:
            logger.error(f"The tool binding failed:{e}")
            raise e

        logger.info(f"I'm calling LLM...")

        #Add detailed stock code tracking log
        logger.info(f"Before LLM calls, ticker parameter: '{ticker}'")
        logger.info(f"Number of messages to LLM:{len(state['messages'])}")

        #🔥 Prints the full content of submissions to the large model
        logger.info("=" * 80)
        logger.info("📝 [Phrasing debugging] Start printing the full content of submissions to the large model")
        logger.info("=" * 80)

        #1. Print system hints
        logger.info("📋 [Phrasing decorated] 1️⃣ System Phrasing (System Message):")
        logger.info("-" * 80)
        logger.info(system_message)
        logger.info("-" * 80)

        #Print a complete reminder template
        logger.info("Full reminder template (Prompt Template):")
        logger.info("-" * 80)
        logger.info(f"Tool name:{', '.join(tool_names)}")
        logger.info(f"Current date:{current_date}")
        logger.info(f"Stock code:{ticker}")
        logger.info(f"Name of company:{company_name}")
        logger.info("-" * 80)

        #3. Print message history
        logger.info("Message History:")
        logger.info("-" * 80)
        for i, msg in enumerate(state['messages']):
            msg_type = type(msg).__name__
            if hasattr(msg, 'content'):
                #Debugging mode: print full, uninterrupted
                content_full = str(msg.content)
                logger.info(f"Message{i+1} [{msg_type}]:")
                logger.info(f"Content Length:{len(content_full)}Character")
                logger.info(f"Content:{content_full}")
            if hasattr(msg, 'tool_calls') and msg.tool_calls:
                logger.info(f"Tool call:{[tc.get('name', 'unknown') for tc in msg.tool_calls]}")
            if hasattr(msg, 'name'):
                logger.info(f"Tool name:{msg.name}")
            logger.info("-" * 40)
        logger.info("-" * 80)

        #Print binding tool information
        logger.info("[Phone debugging]")
        logger.info("-" * 80)
        for i, tool in enumerate(tools):
            tool_name = getattr(tool, 'name', None) or getattr(tool, '__name__', 'unknown')
            tool_desc = getattr(tool, 'description', 'No description')
            logger.info(f"Tools{i+1}: {tool_name}")
            logger.info(f"Description:{tool_desc}")
            if hasattr(tool, 'args_schema'):
                logger.info(f"Parameters:{tool.args_schema}")
            logger.info("-" * 40)
        logger.info("-" * 80)

        logger.info("=" * 80)
        logger.info("📝 [Phrasing debugging] Complete content printing is over, starting to call LLM")
        logger.info("=" * 80)

        #Fix: pass the dictionary instead of the direct message list so that ChatPromptTemplate can handle all variables correctly
        result = chain.invoke({"messages": state["messages"]})
        logger.info(f"[BASIC ANALYSISER]")
        
        #[Debug Log] Print details of AIMESSAGE
        logger.info(f"Amessage details:")
        logger.info(f"- Message type:{type(result).__name__}")
        logger.info(f"- Content length:{len(result.content) if hasattr(result, 'content') else 0}")
        if hasattr(result, 'content') and result.content:
            #Debugging mode: print full, uninterrupted
            logger.info(f"🤖 [Basic Profile Analyst] - Complete content:")
            logger.info(f"{result.content}")
        
        #🔍 [Debug Log] Print tool calls details
        #Detailed recording of LLM returns
        logger.info(f"📊 [BASIC ANALYSISER] = = = LLM returns analysis = = = = =")
        logger.info(f"- Result type:{type(result).__name__}")
        logger.info(f"- Is there a Tool Calls attribute:{hasattr(result, 'tool_calls')}")

        if hasattr(result, 'content'):
            content_preview = str(result.content)[:200] if result.content else "None"
            logger.info(f"- Content length:{len(str(result.content)) if result.content else 0}")
            logger.info(f"📊 [Basic Profile Analyst] - Content preview:{content_preview}...")

        if hasattr(result, 'tool_calls'):
            logger.info(f"- Tool calls:{len(result.tool_calls)}")
            if result.tool_calls:
                logger.info(f"[BASIC ANALYSISTER]{len(result.tool_calls)}Tool call:")
                for i, tc in enumerate(result.tool_calls):
                    logger.info(f"[BASIC ANALYSISER] - Tool call{i+1}: {tc.get('name', 'unknown')} (ID: {tc.get('id', 'unknown')})")
                    if 'args' in tc:
                        logger.info(f"- Parameters:{tc['args']}")
            else:
                logger.info(f"Tool calls is empty list")
        else:
            logger.info(f"[BASIC ANALYSISTER]")

        logger.info(f"== sync, corrected by elderman == @elder man")

        #Use a single Google tool to call for processing Device
        if GoogleToolCallHandler.is_google_model(fresh_llm):
            logger.info(f"📊 [basic profiler] detected the Google model, using a unified tool for processing Device")
            
            #Create Analytic Tips
            analysis_prompt_template = GoogleToolCallHandler.create_analysis_prompt(
                ticker=ticker,
                company_name=company_name,
                analyst_type="基本面分析",
                specific_requirements="重点关注财务数据、盈利能力、估值指标、行业地位等基本面因素。"
            )
            
            #Process Google Model Tool Call
            report, messages = GoogleToolCallHandler.handle_google_tool_calls(
                result=result,
                llm=fresh_llm,
                tools=tools,
                state=state,
                analysis_prompt_template=analysis_prompt_template,
                analyst_name="基本面分析师"
            )

            return {"fundamentals_report": report}
        else:
            #Non-Google processing logic
            logger.debug(f"[DEBUG] Non-Google model{fresh_llm.__class__.__name__}) using standard processing logic")
            
            #Check tool calls
            current_tool_calls = len(result.tool_calls) if hasattr(result, 'tool_calls') else 0
            logger.debug(f"[DBUG] Number of calls for current messages:{current_tool_calls}")
            logger.debug(f"[DBUG] Cumulative tool call times:{tool_call_count}/{max_tool_calls}")

            if current_tool_calls > 0:
                #Check if the tool has been called.
                messages = state.get("messages", [])
                has_tool_result = any(isinstance(msg, ToolMessage) for msg in messages)

                if has_tool_result:
                    #There's already a tool result, LLM should no longer call the tool and force the report.
                    logger.warning(f"The ⚠️ [compulsory generation report] tool returned the data, but the LLM still tried to call the tool to force the generation of the report based on existing data")

                    #Create a special mandatory report alert (without reference to tools)
                    force_system_prompt = (
                        f"你是专业的股票基本面分析师。"
                        f"你已经收到了股票 {company_name}（代码：{ticker}）的基本面数据。"
                        f"🚨 现在你必须基于这些数据生成完整的基本面分析报告！🚨\n\n"
                        f"报告必须包含以下内容：\n"
                        f"1. 公司基本信息和财务数据分析\n"
                        f"2. PE、PB、PEG等估值指标分析\n"
                        f"3. 当前股价是否被低估或高估的判断\n"
                        f"4. 合理价位区间和目标价位建议\n"
                        f"5. 基于基本面的投资建议（买入/持有/卖出）\n\n"
                        f"要求：\n"
                        f"- 使用中文撰写报告\n"
                        f"- 基于消息历史中的真实数据进行分析\n"
                        f"- 分析要详细且专业\n"
                        f"- 投资建议必须明确（买入/持有/卖出）"
                    )

                    #Create a special reminder template (without binding tool)
                    force_prompt = ChatPromptTemplate.from_messages([
                        ("system", force_system_prompt),
                        MessagesPlaceholder(variable_name="messages"),
                    ])

                    #Do not bind tools, force LLM to generate text
                    force_chain = force_prompt | fresh_llm

                    logger.info(f"🔧 [compulsory generation of reports] Re-call LLM with a specific reminder...")
                    force_result = force_chain.invoke({"messages": messages})

                    report = str(force_result.content) if hasattr(force_result, 'content') else "基本面分析完成"
                    logger.info(f"✅ [compulsory generation of reports] Successfully generating reports, length:{len(report)}Character")

                    return {
                        "fundamentals_report": report,
                        "messages": [force_result],
                        "fundamentals_tool_call_count": tool_call_count
                    }

                elif tool_call_count >= max_tool_calls:
                    #Maximum number of calls reached, but no tool results available (should not occur)
                    logger.warning(f"🔧 [Aberrant] Maximum tool call times{max_tool_calls}But no tool results.")
                    fallback_report = f"基本面分析（股票代码：{ticker}）\n\n由于达到最大工具调用次数限制，使用简化分析模式。建议检查数据源连接或降低分析复杂度。"
                    return {
                        "messages": [result],
                        "fundamentals_report": fallback_report,
                        "fundamentals_tool_call_count": tool_call_count
                    }
                else:
                    #First call tool, normal process
                    logger.info(f"== sync, corrected by elderman == @elder man")
                    tool_calls_info = []
                    for tc in result.tool_calls:
                        tool_calls_info.append(tc['name'])
                        logger.debug(f"[DBUG] Tool call{len(tool_calls_info)}: {tc}")

                    logger.info(f"📊 [normal process] LLM requests a call tool:{tool_calls_info}")
                    logger.info(f"Number of calls for the [normal process] tool:{len(tool_calls_info)}")
                    logger.info(f"📊 [normal process] returns state, awaiting tool execution")
                    #Attention: Don't add a counter here!
                    #The counter should not be added until the tool has been implemented (the next time we enter the analyst node).
                    return {
                        "messages": [result]
                    }
            else:
                #No tool to call, check if mandatory call is needed
                logger.info(f"== sync, corrected by elderman == @elder man")
                logger.debug(f"📊 [DEBUG] Detects that the model is not calling a tool and checks whether the call is mandatory")

                #Option 1: Check if there are data in the message history that are returned with tools
                messages = state.get("messages", [])
                logger.info(f"[Information history]{len(messages)}")

                #Number of messages by type
                ai_message_count = sum(1 for msg in messages if isinstance(msg, AIMessage))
                tool_message_count = sum(1 for msg in messages if isinstance(msg, ToolMessage))
                logger.info(f"AIMESSAGE:{ai_message_count}, ToolMessage Number:{tool_message_count}")

                #Type of recording of recent messages
                recent_messages = messages[-5:] if len(messages) >= 5 else messages
                logger.info(f"[indistinct chatter]{len(recent_messages)}Can not open message{[type(msg).__name__ for msg in recent_messages]}")

                has_tool_result = any(isinstance(msg, ToolMessage) for msg in messages)
                logger.info(f"[Check results]{has_tool_result}")

                #Option 2: Check if AIMESSAGE has an analytical component
                has_analysis_content = False
                if hasattr(result, 'content') and result.content:
                    content_length = len(str(result.content))
                    logger.info(f"[ content check] LLM returns content length:{content_length}Character")
                    #Considers as valid analytical content if content length exceeds 500 characters
                    if content_length > 500:
                        has_analysis_content = True
                        logger.info(f"✅ [ content check] LLM has returned to valid analysis of content (long:{content_length}Character > 500 character threshold)")
                    else:
                        logger.info(f"⚠️ [ content check] LLM returns with a shorter content (long:{content_length}Character < 500 character threshold)")
                else:
                    logger.info(f"⚠️ [ content check] LLM does not return content or content empty")

                #Programme 3: Number of calls for statistical tools
                tool_call_count = sum(1 for msg in messages if isinstance(msg, ToolMessage))
                logger.info(f"[Statistical] Number of historical tools called:{tool_call_count}")

                logger.info(f"Summary - Tool results:{tool_call_count}, has tool results:{has_tool_result}, already analysed:{has_analysis_content}")
                logger.info(f"📊 [Basic Analyser]")

                #Skip mandatory call if tool results or analytical content already exist
                if has_tool_result or has_analysis_content:
                    logger.info(f"🚫 [Decision] = = Skip Force Call = = = = =")
                    if has_tool_result:
                        logger.info(f"[Decision reasons]{tool_call_count}Sub-tool calls results to avoid duplication of calls")
                    if has_analysis_content:
                        logger.info(f"⚠️ [decision reasons] LLM has returned to valid analysis without having to use mandatory tools")

                    #Report directly from LLM returns
                    report = str(result.content) if hasattr(result, 'content') else "基本面分析完成"
                    logger.info(f"📊 [return result] Use LLM returned analysis, report length:{len(report)}Character")
                    logger.info(f"📊 [returns results] Report preview (first 200 characters):{report[:200]}...")
                    logger.info(f"[Decision] Basic analysis completed, skip repeated calls successful")

                    #🔧 Keeps the tool call counter unchanged (updated at the beginning on ToolMessage)
                    return {
                        "fundamentals_report": report,
                        "messages": [result],
                        "fundamentals_tool_call_count": tool_call_count
                    }

                #Forced call if no tool results and no analytical content
                logger.info(f"🔧 [decision] = = = enforce mandatory tool call = = = = =")
                logger.info(f"🔧 [decision reasons] No tool results or analytical content detected, basic data required")
                logger.info(f"[Decision] Enable mandatory tool call mode")

                #Forced access to the Unified Basic Analysis Tool
                try:
                    logger.debug(f"[DEBUG] Forced call get stock fundamentals unified...")
                    #Securely search for a unified fundamental analysis tool
                    unified_tool = None
                    for tool in tools:
                        tool_name = None
                        if hasattr(tool, 'name'):
                            tool_name = tool.name
                        elif hasattr(tool, '__name__'):
                            tool_name = tool.__name__

                        if tool_name == 'get_stock_fundamentals_unified':
                            unified_tool = tool
                            break
                    if unified_tool:
                        logger.info(f"🔍 [Tool Call] Find a unified tool, ready for mandatory call")
                        logger.info(f"[Tool Call]{ticker}', start_date: {start_date}, end_date: {current_date}")

                        combined_data = unified_tool.invoke({
                            'ticker': ticker,
                            'start_date': start_date,
                            'end_date': current_date,
                            'curr_date': current_date
                        })

                        logger.info(f"[Tool Call]")
                        logger.info(f"[Tool Call] Returns data length:{len(combined_data)}Character")
                        logger.debug(f"📊 [DBUG] Harmonization tool data acquisition success, length:{len(combined_data)}Character")
                        #Write the data returned from the unified tool to the log for ease of searching and analysis
                        try:
                            if isinstance(combined_data, (dict, list)):
                                import json
                                _preview = json.dumps(combined_data, ensure_ascii=False, default=str)
                                _full = _preview
                            else:
                                _preview = str(combined_data)
                                _full = _preview

                            #Preview information control length to avoid excessive logs
                            _preview_truncated = (_preview[:6000] + ("..." if len(_preview) > 2000 else ""))
                            logger.info(f"📦 [basic profiler] Unified tool returns the data preview (front 6,000 characters):\n{_preview_truncated}")
                            #Full data written to DEBUG level
                            logger.debug(f"🧾 [Basic Analyser] Unified tool returns complete data: \n{_full}")
                        except Exception as _log_err:
                            logger.warning(f"⚠️ [basic analyst] Error recording UAT data:{_log_err}")
                    else:
                        combined_data = "统一基本面分析工具不可用"
                        logger.debug(f"[DEBUG] Unified tool not found")
                except Exception as e:
                    combined_data = f"统一基本面分析工具调用失败: {e}"
                    logger.debug(f"[DEBUG] Uniform tool call anomalies:{e}")
                
                currency_info = f"{market_info['currency_name']}（{market_info['currency_symbol']}）"
                
                #Generate analysis based on real data
                analysis_prompt = f"""基于以下真实数据，对{company_name}（股票代码：{ticker}）进行详细的基本面分析：

{combined_data}

请提供：
1. 公司基本信息分析（{company_name}，股票代码：{ticker}）
2. 财务状况评估
3. 盈利能力分析
4. 估值分析（使用{currency_info}）
5. 投资建议（买入/持有/卖出）

要求：
- 基于提供的真实数据进行分析
- 正确使用公司名称"{company_name}"和股票代码"{ticker}"
- 价格使用{currency_info}
- 投资建议使用中文
- 分析要详细且专业"""

                try:
                    #Create simple analytical chains
                    analysis_prompt_template = ChatPromptTemplate.from_messages([
                        ("system", "你是专业的股票基本面分析师，基于提供的真实数据进行分析。"),
                        ("human", "{analysis_request}")
                    ])
                    
                    analysis_chain = analysis_prompt_template | fresh_llm
                    analysis_result = analysis_chain.invoke({"analysis_request": analysis_prompt})
                    
                    if hasattr(analysis_result, 'content'):
                        report = analysis_result.content
                    else:
                        report = str(analysis_result)

                    logger.info(f"📊 [Basic Analyser] Forced tool call complete, report length:{len(report)}")

                except Exception as e:
                    logger.error(f"[DBUG] Force tool call analysis failed:{e}")
                    report = f"基本面分析失败：{str(e)}"

                #🔧 Keeps the tool call counter unchanged (updated at the beginning on ToolMessage)
                return {
                    "fundamentals_report": report,
                    "fundamentals_tool_call_count": tool_call_count
                }

        #It's not supposed to be here, but as backup.
        logger.debug(f"[DBUG] returns: fundmentals report length={len(result.content) if hasattr(result, 'content') else 0}")
        #🔧 Keeps the tool call counter unchanged (updated at the beginning on ToolMessage)
        return {
            "messages": [result],
            "fundamentals_report": result.content if hasattr(result, 'content') else str(result),
            "fundamentals_tool_call_count": tool_call_count
        }

    return fundamentals_analyst_node
