# TradingAgents/graph/conditional_logic.py

from tradingagents.agents.utils.agent_states import AgentState

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("default")


class ConditionalLogic:
    """Handles conditional logic for determining graph flow."""

    def __init__(self, max_debate_rounds=1, max_risk_discuss_rounds=1):
        """Initialize with configuration parameters."""
        self.max_debate_rounds = max_debate_rounds
        self.max_risk_discuss_rounds = max_risk_discuss_rounds

    def should_continue_market(self, state: AgentState):
        """Determine if market analysis should continue."""
        from tradingagents.utils.logging_init import get_logger
        logger = get_logger("agents")

        messages = state["messages"]
        last_message = messages[-1]

        #Death Cycle Restoration: Add Tool Call Number Check
        tool_call_count = state.get("market_tool_call_count", 0)
        max_tool_calls = 3

        #Check if market analysis is available
        market_report = state.get("market_report", "")

        logger.info(f"[Continues]")
        logger.info(f"🔀 [conditional judgement] - Number of messages:{len(messages)}")
        logger.info(f"- Report length:{len(market_report)}")
        logger.info(f"🔧 [ Dead Cycle Restoration] - Number of tools called:{tool_call_count}/{max_tool_calls}")
        logger.info(f"- Last message type:{type(last_message).__name__}")
        logger.info(f"- Is there a tool calls:{hasattr(last_message, 'tool_calls')}")
        if hasattr(last_message, 'tool_calls'):
            logger.info(f"[Conditions] - Tool calls:{len(last_message.tool_calls) if last_message.tool_calls else 0}")
            if last_message.tool_calls:
                for i, tc in enumerate(last_message.tool_calls):
                    logger.info(f"- Tool call.{i}]: {tc.get('name', 'unknown')}")

        #Death cycle repair: Forced termination if maximum number of tools called
        if tool_call_count >= max_tool_calls:
            logger.warning(f"🔧 [Death Cycle Restoration] Maximum number of tools called, forced end:")
            return "Msg Clear Market"

        #If already reported, indicate that the analysis has been completed, not recycled
        if market_report and len(market_report) > 100:
            logger.info(f"Report completed. Return:")
            return "Msg Clear Market"

        #Only AIMESSAGE has tool calls properties
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            logger.info(f"Tool calls, return:")
            return "tools_market"

        logger.info(f"🔀 [Conditions] ✅ No tool calls, return:")
        return "Msg Clear Market"

    def should_continue_social(self, state: AgentState):
        """Determine if social media analysis should continue."""
        from tradingagents.utils.logging_init import get_logger
        logger = get_logger("agents")

        messages = state["messages"]
        last_message = messages[-1]

        #Death Cycle Restoration: Add Tool Call Number Check
        tool_call_count = state.get("sentiment_tool_call_count", 0)
        max_tool_calls = 3

        #Check for emotional analysis.
        sentiment_report = state.get("sentiment_report", "")

        logger.info(f"[Continues]")
        logger.info(f"🔀 [conditional judgement] - Number of messages:{len(messages)}")
        logger.info(f"- Report length:{len(sentiment_report)}")
        logger.info(f"🔧 [ Dead Cycle Restoration] - Number of tools called:{tool_call_count}/{max_tool_calls}")

        #Death cycle repair: Forced termination if maximum number of tools called
        if tool_call_count >= max_tool_calls:
            logger.warning(f"🔧 [Recovery of death cycle] Maximum number of tools to call, forced end:")
            return "Msg Clear Social"

        #If already reported, indicate that the analysis has been completed, not recycled
        if sentiment_report and len(sentiment_report) > 100:
            logger.info(f"Report completed. Return:")
            return "Msg Clear Social"

        #Only AIMESSAGE has tool calls properties
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            logger.info(f"Tool calls, return:")
            return "tools_social"

        logger.info(f"[Conditions]")
        return "Msg Clear Social"

    def should_continue_news(self, state: AgentState):
        """Determine if news analysis should continue."""
        from tradingagents.utils.logging_init import get_logger
        logger = get_logger("agents")

        messages = state["messages"]
        last_message = messages[-1]

        #Death Cycle Restoration: Add Tool Call Number Check
        tool_call_count = state.get("news_tool_call_count", 0)
        max_tool_calls = 3

        #Check if there's a press analysis.
        news_report = state.get("news_report", "")

        logger.info(f"[Continues]")
        logger.info(f"🔀 [conditional judgement] - Number of messages:{len(messages)}")
        logger.info(f"- Report length:{len(news_report)}")
        logger.info(f"🔧 [ Dead Cycle Restoration] - Number of tools called:{tool_call_count}/{max_tool_calls}")

        #Death cycle repair: Forced termination if maximum number of tools called
        if tool_call_count >= max_tool_calls:
            logger.warning(f"🔧 [ Dead Cycle Restoration] Maximum tool call, forced end:")
            return "Msg Clear News"

        #If already reported, indicate that the analysis has been completed, not recycled
        if news_report and len(news_report) > 100:
            logger.info(f"Report completed. Return:")
            return "Msg Clear News"

        #Only AIMESSAGE has tool calls properties
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            logger.info(f"Tool calls, returns:")
            return "tools_news"

        logger.info(f"🔀 [Conditions] ✅ Notool calls, return:")
        return "Msg Clear News"

    def should_continue_fundamentals(self, state: AgentState):
        """To determine whether fundamental analysis should continue"""
        from tradingagents.utils.logging_init import get_logger
        logger = get_logger("agents")

        messages = state["messages"]
        last_message = messages[-1]

        #Death Cycle Restoration: Add Tool Call Number Check
        tool_call_count = state.get("fundamentals_tool_call_count", 0)
        max_tool_calls = 1  #One tool call will get all the data.

        #Check for basic reports.
        fundamentals_report = state.get("fundamentals_report", "")

        logger.info(f"[Continue]")
        logger.info(f"🔀 [conditional judgement] - Number of messages:{len(messages)}")
        logger.info(f"- Report length:{len(fundamentals_report)}")
        logger.info(f"🔧 [ Dead Cycle Restoration] - Number of tools called:{tool_call_count}/{max_tool_calls}")
        logger.info(f"- Last message type:{type(last_message).__name__}")
        
        #🔍 [Debug Log] Print details of the last message
        logger.info(f"[Conditions rule]")
        logger.info(f"- Message type:{type(last_message).__name__}")
        if hasattr(last_message, 'content'):
            content_preview = last_message.content[:300] + "..." if len(last_message.content) > 300 else last_message.content
            logger.info(f"🤖 [conditional judgement] - Content preview:{content_preview}")
        
        #🔍 [Debug Log] Print tool calls details
        logger.info(f"- Is there a tool calls:{hasattr(last_message, 'tool_calls')}")
        if hasattr(last_message, 'tool_calls'):
            logger.info(f"[Conditions] - Tool calls:{len(last_message.tool_calls) if last_message.tool_calls else 0}")
            if last_message.tool_calls:
                logger.info(f"[Conditions rule]{len(last_message.tool_calls)}Tool call:")
                for i, tc in enumerate(last_message.tool_calls):
                    logger.info(f"[Conditions] - Tool call{i+1}: {tc.get('name', 'unknown')} (ID: {tc.get('id', 'unknown')})")
                    if 'args' in tc:
                        logger.info(f"- Parameters:{tc['args']}")
            else:
                logger.info(f"🔧 [conditional judgement] tool calls are empty lists")
        else:
            logger.info(f"🔧 [conditional judgement] No tool calls attribute")

        #✅ Priority 1: No recycling if the analysis has been completed
        if fundamentals_report and len(fundamentals_report) > 100:
            logger.info(f"Report completed. Return:")
            return "Msg Clear Fundamentals"

        #Priority 2: Tool calls, go to the tool.
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            #Check if the maximum number of calls is exceeded
            if tool_call_count >= max_tool_calls:
                logger.warning(f"🔧 [Death Cycle Restoration] Tool has reached maximum number of calls ({tool_call_count}/{max_tool_calls}But there's still tool calls, forced end")
                return "Msg Clear Fundamentals"

            logger.info(f"🔀 [Conditions judgement] 🔧 @tool calls, returns:")
            return "tools_fundamentals"

        #Priority 3: No tool calls.
        logger.info(f"[Conditions] 🔀 No tool calls, return:")
        return "Msg Clear Fundamentals"

    def should_continue_debate(self, state: AgentState) -> str:
        """Determine if debate should continue."""
        current_count = state["investment_debate_state"]["count"]
        max_count = 2 * self.max_debate_rounds
        current_speaker = state["investment_debate_state"]["current_response"]

        #Detailed log
        logger.info(f"Number of current statements:{current_count}, Max:{max_count}(configuration wheel:{self.max_debate_rounds})")
        logger.info(f"Current speakers:{current_speaker}")

        if current_count >= max_count:
            logger.info(f"- Research Manager")
            return "Research Manager"

        next_speaker = "Bear Researcher" if current_speaker.startswith("Bull") else "Bull Researcher"
        logger.info(f"[Investment debate controls]{next_speaker}")
        return next_speaker

    def should_continue_risk_analysis(self, state: AgentState) -> str:
        """Determine if risk analysis should continue."""
        current_count = state["risk_debate_state"]["count"]
        max_count = 3 * self.max_risk_discuss_rounds
        latest_speaker = state["risk_debate_state"]["latest_speaker"]

        #Detailed log
        logger.info(f"[Risk discussion controls]{current_count}, Max:{max_count}(configuration wheel:{self.max_risk_discuss_rounds})")
        logger.info(f"[Risk Discussion Control]{latest_speaker}")

        if current_count >= max_count:
            logger.info(f"[Risk Discussion Controls]")
            return "Risk Judge"

        #Identification of next speaker
        if latest_speaker.startswith("Risky"):
            next_speaker = "Safe Analyst"
        elif latest_speaker.startswith("Safe"):
            next_speaker = "Neutral Analyst"
        else:
            next_speaker = "Risky Analyst"

        logger.info(f"[Risk Discussion Control]{next_speaker}")
        return next_speaker
