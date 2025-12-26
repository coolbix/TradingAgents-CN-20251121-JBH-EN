import time
import json

#Import Unified Log System
from tradingagents.utils.logging_init import get_logger
logger = get_logger("default")


def create_risk_manager(llm, memory):
    def risk_manager_node(state) -> dict:

        company_name = state["company_of_interest"]

        history = state["risk_debate_state"]["history"]
        risk_debate_state = state["risk_debate_state"]
        market_research_report = state["market_report"]
        news_report = state["news_report"]
        fundamentals_report = state["news_report"]
        sentiment_report = state["sentiment_report"]
        trader_plan = state["investment_plan"]

        curr_situation = f"{market_research_report}\n\n{sentiment_report}\n\n{news_report}\n\n{fundamentals_report}"

        #Security check: ensure memory is not None
        if memory is not None:
            past_memories = memory.get_memories(curr_situation, n_matches=2)
        else:
            logger.warning(f"[DEBUG] memory is None, skip historical memory search")
            past_memories = []

        past_memory_str = ""
        for i, rec in enumerate(past_memories, 1):
            past_memory_str += rec["recommendation"] + "\n\n"

        prompt = f"""作为风险管理委员会主席和辩论主持人，您的目标是评估三位风险分析师——激进、中性和安全/保守——之间的辩论，并确定交易员的最佳行动方案。您的决策必须产生明确的建议：买入、卖出或持有。只有在有具体论据强烈支持时才选择持有，而不是在所有方面都似乎有效时作为后备选择。力求清晰和果断。

决策指导原则：
1. **总结关键论点**：提取每位分析师的最强观点，重点关注与背景的相关性。
2. **提供理由**：用辩论中的直接引用和反驳论点支持您的建议。
3. **完善交易员计划**：从交易员的原始计划**{trader_plan}**开始，根据分析师的见解进行调整。
4. **从过去的错误中学习**：使用**{past_memory_str}**中的经验教训来解决先前的误判，改进您现在做出的决策，确保您不会做出错误的买入/卖出/持有决定而亏损。

交付成果：
- 明确且可操作的建议：买入、卖出或持有。
- 基于辩论和过去反思的详细推理。

---

**分析师辩论历史：**
{history}

---

专注于可操作的见解和持续改进。建立在过去经验教训的基础上，批判性地评估所有观点，确保每个决策都能带来更好的结果。请用中文撰写所有分析内容和建议。"""

        #Statistics prompt size
        prompt_length = len(prompt)
        #Roughly estimated number of tokens (approximately 1.5-2 characters/token in Chinese, approximately 4 characters/token in English)
        estimated_tokens = int(prompt_length / 1.8)  #Conservative estimate

        logger.info(f"[Risk Manager] Prompt Statistics:")
        logger.info(f"- The length of the debate:{len(history)}Character")
        logger.info(f"- Trader ' s planned length:{len(trader_plan)}Character")
        logger.info(f"- Length of historical memory:{len(past_memory_str)}Character")
        logger.info(f"- Total Prompt length:{prompt_length}Character")
        logger.info(f"- Estimating input Token: ~{estimated_tokens} tokens")

        #Enhanced LLM calls with error-processing and retry mechanisms
        max_retries = 3
        retry_count = 0
        response_content = ""

        while retry_count < max_retries:
            try:
                logger.info(f"[Risk Manager] Call LLM to generate transactional decisions (attempted){retry_count + 1}/{max_retries})")

                #Record time
                start_time = time.time()

                response = llm.invoke(prompt)

                #End of record
                elapsed_time = time.time() - start_time
                
                if response and hasattr(response, 'content') and response.content:
                    response_content = response.content.strip()

                    #Statistical response information
                    response_length = len(response_content)
                    estimated_output_tokens = int(response_length / 1.8)

                    #Try to get the actual token usage (if LLM returns)
                    usage_info = ""
                    if hasattr(response, 'response_metadata') and response.response_metadata:
                        metadata = response.response_metadata
                        if 'token_usage' in metadata:
                            token_usage = metadata['token_usage']
                            usage_info = f", 实际Token: 输入={token_usage.get('prompt_tokens', 'N/A')} 输出={token_usage.get('completion_tokens', 'N/A')} 总计={token_usage.get('total_tokens', 'N/A')}"

                    logger.info(f"[Risk Manager] LLM calls time:{elapsed_time:.2f}sec")
                    logger.info(f"[Risk Manager]{response_length}Character, estimate?{estimated_output_tokens} tokens{usage_info}")

                    if len(response_content) > 10:  #Ensuring that responses are substantive
                        logger.info(f"[Risk Manager] LLM called successfully.")
                        break
                    else:
                        logger.warning(f"[Risk Manager] LLM response is too short:{len(response_content)}Character")
                        response_content = ""
                else:
                    logger.warning(f"[Risk Manager] LLM response is empty or invalid")
                    response_content = ""

            except Exception as e:
                elapsed_time = time.time() - start_time
                logger.error(f"[Risk Manager] LLM Call Failed (Trying){retry_count + 1}): {str(e)}")
                logger.error(f"[Risk Manager]{elapsed_time:.2f}sec")
                response_content = ""
            
            retry_count += 1
            if retry_count < max_retries and not response_content:
                logger.info(f"[Risk Manager]")
                time.sleep(2)
        
        #If all retries fail, create default decisions
        if not response_content:
            logger.error(f"[Risk Manager] All LLM calls failed, using default decision-making")
            response_content = f"""**默认建议：持有**

由于技术原因无法生成详细分析，基于当前市场状况和风险控制原则，建议对{company_name}采取持有策略。

**理由：**
1. 市场信息不足，避免盲目操作
2. 保持现有仓位，等待更明确的市场信号
3. 控制风险，避免在不确定性高的情况下做出激进决策

**建议：**
- 密切关注市场动态和公司基本面变化
- 设置合理的止损和止盈位
- 等待更好的入场或出场时机

注意：此为系统默认建议，建议结合人工分析做出最终决策。"""

        new_risk_debate_state = {
            "judge_decision": response_content,
            "history": risk_debate_state["history"],
            "risky_history": risk_debate_state["risky_history"],
            "safe_history": risk_debate_state["safe_history"],
            "neutral_history": risk_debate_state["neutral_history"],
            "latest_speaker": "Judge",
            "current_risky_response": risk_debate_state["current_risky_response"],
            "current_safe_response": risk_debate_state["current_safe_response"],
            "current_neutral_response": risk_debate_state["current_neutral_response"],
            "count": risk_debate_state["count"],
        }

        logger.info(f"[Risk Manager] Final decision-making completed, content length:{len(response_content)}Character")
        
        return {
            "risk_debate_state": new_risk_debate_state,
            "final_trade_decision": response_content,
        }

    return risk_manager_node
