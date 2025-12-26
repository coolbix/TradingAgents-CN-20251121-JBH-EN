#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Google Modelling Tool Call Unified Processing Device

To solve the Google model's problem of emptying the tool when it is called,
Provide a unified tool to call processing logic for all analysts.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from langchain_core.messages import HumanMessage, ToolMessage, AIMessage

logger = logging.getLogger(__name__)

class GoogleToolCallHandler:
    """Google Modelling Tool Call Unified Processing Device"""
    
    @staticmethod
    def is_google_model(llm) -> bool:
        """Check for Google model"""
        return 'Google' in llm.__class__.__name__ or 'ChatGoogleOpenAI' in llm.__class__.__name__
    
    @staticmethod
    def handle_google_tool_calls(
        result: AIMessage,
        llm: Any,
        tools: List[Any],
        state: Dict[str, Any],
        analysis_prompt_template: str,
        analyst_name: str = "分析师"
    ) -> Tuple[str, List[Any]]:
        """Harmonization of tools for processing Google models

Args:
result: LLM first call
llm: example of a language model
lists: available tools
State: Current status
Analysis prompt template: Analytictography Board
analyser's name

Returns:
Tuple [str, List]: (analytical, message list)
"""
        
        #Validate input parameters
        logger.info(f"[{analyst_name}Start Google Tool Call Processing...")
        logger.debug(f"[{analyst_name}🔍LLMtype:{llm.__class__.__name__}")
        logger.debug(f"[{analyst_name}Number of tools: 🔍{len(tools) if tools else 0}")
        logger.debug(f"[{analyst_name}State type: 🔍{type(state).__name__ if state else None}")
        
        if not GoogleToolCallHandler.is_google_model(llm):
            logger.warning(f"[{analyst_name}Non-Google model, skip special treatment")
            logger.debug(f"[{analyst_name}Model check failed:{llm.__class__.__name__}")
            #Non-Google model, return original content
            return result.content, [result]
        
        logger.info(f"[{analyst_name}Confirm it as Google Model.")
        logger.debug(f"[{analyst_name}Results type:{type(result).__name__}")
        logger.debug(f"[{analyst_name}Results properties:{[attr for attr in dir(result) if not attr.startswith('_')]}")
        
        #Check if API calls are successful
        if not hasattr(result, 'content'):
            logger.error(f"[{analyst_name}Google Model API call failed, no return content")
            logger.debug(f"[{analyst_name}🔍The result object lacks a content attribute")
            return "Google模型API调用失败", []
        
        #Check for tools to call
        if not hasattr(result, 'tool_calls'):
            logger.warning(f"[{analyst_name}The result object has no tool calls properties")
            logger.debug(f"[{analyst_name}Available Properties:{[attr for attr in dir(result) if not attr.startswith('_')]}")
            return result.content, [result]
        
        if not result.tool_calls:
            #Improved: more detailed diagnostic information provided
            logger.info(f"[{analyst_name}The Google model did not call on the tool, possibly because:")
            logger.info(f"[{analyst_name} - Input message empty or format incorrect")
            logger.info(f"[{analyst_name}- The model does not consider the call tool necessary")
            logger.info(f"[{analyst_name}- Possible problems with tool binding")
            
            #Check input messages
            if "messages" in state:
                messages = state["messages"]
                if not messages:
                    logger.warning(f"[{analyst_name}Input message list is empty")
                else:
                    logger.info(f"[{analyst_name}Number of messages entered:{len(messages)}")
                    for i, msg in enumerate(messages):
                        msg_type = type(msg).__name__
                        content_preview = str(msg.content)[:100] if hasattr(msg, 'content') else "无内容"
                        logger.info(f"[{analyst_name}Message{i+1}: {msg_type} - {content_preview}...")
            
            #Check if the contents are analytical
            content = result.content
            logger.info(f"[{analyst_name}Check if return contents are analytical...")
            logger.debug(f"[{analyst_name}Content type: 🔍{type(content)}")
            logger.debug(f"[{analyst_name}Content length:{len(content) if content else 0}")
            
            #Check if content contains features of the analysis
            is_analysis_report = False
            analysis_keywords = ["分析", "报告", "总结", "评估", "建议", "风险", "趋势", "市场", "股票", "投资"]
            
            if content:
                #Check content length and keywords
                if len(content) > 200:  #Assuming that the analysis report has at least 200 words Arguments
                    keyword_count = sum(1 for keyword in analysis_keywords if keyword in content)
                    is_analysis_report = keyword_count >= 3  #It contains at least three keywords.
                
                logger.info(f"[{analyst_name}🔍 >The content of the judgement is{'Analytical reports' if is_analysis_report else 'Non-analytical'}")
                
                if is_analysis_report:
                    logger.info(f"[{analyst_name}The Google model returned the analysis directly, length:{len(content)}Character")
                    return content, [result]
            
            #returns original content, but adds instructions
            return result.content, [result]
        
        logger.info(f"[{analyst_name}The Google model has been activated.{len(result.tool_calls)}A tool")
        
        #Recording tool call details
        for i, tool_call in enumerate(result.tool_calls):
            logger.info(f"[{analyst_name}Tools{i+1}:")
            logger.info(f"[{analyst_name}]   ID: {tool_call.get('id', 'N/A')}")
            logger.info(f"[{analyst_name}Name:{tool_call.get('name', 'N/A')}")
            logger.info(f"[{analyst_name}] Parameters:{tool_call.get('args', {})}")
        
        try:
            #Execute Tool Call
            tool_messages = []
            tool_results = []
            executed_tools = set()  #Prevent repeated calls for the same tool
            
            logger.info(f"[{analyst_name}Start now.{len(result.tool_calls)}Tool Call...")
            
            #Authentication Tool Call Format
            valid_tool_calls = []
            for i, tool_call in enumerate(result.tool_calls):
                if GoogleToolCallHandler._validate_tool_call(tool_call, i, analyst_name):
                    valid_tool_calls.append(tool_call)
                else:
                    #Try to fix tool calls
                    fixed_tool_call = GoogleToolCallHandler._fix_tool_call(tool_call, i, analyst_name)
                    if fixed_tool_call:
                        valid_tool_calls.append(fixed_tool_call)
            
            logger.info(f"[{analyst_name}Effective tools to call:{len(valid_tool_calls)}/{len(result.tool_calls)}")
            
            for i, tool_call in enumerate(valid_tool_calls):
                tool_name = tool_call.get('name')
                tool_args = tool_call.get('args', {})
                tool_id = tool_call.get('id')
                
                #Prevention of repeated calls for the same tool (in particular, uniform market data tools)
                tool_signature = f"{tool_name}_{hash(str(tool_args))}"
                if tool_signature in executed_tools:
                    logger.warning(f"[{analyst_name}Skip repeated tool calls:{tool_name}")
                    continue
                executed_tools.add(tool_signature)
                
                logger.info(f"[{analyst_name}Implementation tool{i+1}/{len(valid_tool_calls)}: {tool_name}")
                logger.info(f"[{analyst_name}] Parameters:{tool_args}")
                logger.debug(f"[{analyst_name}🔧 Tool call details:{tool_call}")
                
                #Find corresponding tools and execute them
                tool_result = None
                available_tools = []
                
                for tool in tools:
                    current_tool_name = GoogleToolCallHandler._get_tool_name(tool)
                    available_tools.append(current_tool_name)
                    
                    if current_tool_name == tool_name:
                        try:
                            logger.debug(f"[{analyst_name}Find tools:{tool.__class__.__name__}")
                            logger.debug(f"[{analyst_name}Tool type check...")
                            
                            #Check tool types and call them accordingly
                            if hasattr(tool, 'invoke'):
                                #LangChain tool, using invoke method
                                logger.info(f"[{analyst_name}🚀 is calling the Langchain tool.infoke()...")
                                tool_result = tool.invoke(tool_args)
                                logger.info(f"[{analyst_name}✅Langchain tool successfully implemented, result length:{len(str(tool_result))}Character")
                                logger.debug(f"[{analyst_name}🔧 Tool Result Type:{type(tool_result)}")
                            elif callable(tool):
                                #Normal Python function, direct call
                                logger.info(f"[{analyst_name}The Python function tool is being called...")
                                tool_result = tool(**tool_args)
                                logger.info(f"[{analyst_name}✅Python function tool successfully executed, result length:{len(str(tool_result))}Character")
                                logger.debug(f"[{analyst_name}🔧 Tool Result Type:{type(tool_result)}")
                            else:
                                logger.error(f"[{analyst_name}The following tool types are not supported:{type(tool)}")
                                tool_result = f"工具类型不支持: {type(tool)}"
                            break
                        except Exception as tool_error:
                            logger.error(f"[{analyst_name}The tool failed:{tool_error}")
                            logger.error(f"[{analyst_name}Unusual type:{type(tool_error).__name__}")
                            logger.error(f"[{analyst_name}Unusual details:{str(tool_error)}")
                            
                            #Record details of abnormal stacks
                            import traceback
                            error_traceback = traceback.format_exc()
                            logger.error(f"[{analyst_name}The tool executes abnormal stacks:{error_traceback}")
                            
                            tool_result = f"工具执行失败: {str(tool_error)}"
                
                logger.debug(f"[{analyst_name}List of tools available:{available_tools}")
                
                if tool_result is None:
                    tool_result = f"未找到工具: {tool_name}"
                    logger.warning(f"[{analyst_name}No tools found:{tool_name}")
                    logger.debug(f"[{analyst_name}The name of the tool does not match and it is expected that:{tool_name}, available:{available_tools}")
                
                #Create Tool Message
                tool_message = ToolMessage(
                    content=str(tool_result),
                    tool_call_id=tool_id
                )
                tool_messages.append(tool_message)
                tool_results.append(tool_result)
                logger.debug(f"[{analyst_name}Create Tool Message, ID:{tool_message.tool_call_id}")
            
            logger.info(f"[{analyst_name}🔧 Tool call complete. Success:{len(tool_results)}Total:{len(result.tool_calls)}")
            
            #Second call model generation final analysis report
            logger.info(f"[{analyst_name}🚀 Generate final analysis based on the results of the tool...")
            
            #🔧 [Optimate] Do not accumulate historical news, only keep the information needed for the current analysis
            #Reason:
            #1. Historical information of fundamental analysts is not required from other analysts
            #2. Avoid excessive message (accumulated to 55,096 characters)
            #3. Token consumption and cost reduction
            #4. Following up, Research Manager consolidates the reports of all analysts
            safe_messages = []

            #Only keep initial user messages (if any)
            if "messages" in state and state["messages"]:
                #Keep only the first of these, HumanMessage.
                for msg in state["messages"]:
                    if isinstance(msg, HumanMessage):
                        safe_messages.append(msg)
                        logger.debug(f"[{analyst_name}Can not open message")
                        break

            #Add the current result (Ai Tool Call)
            if hasattr(result, 'content'):
                safe_messages.append(result)
                logger.debug(f"[{analyst_name}Add AI Tool Call Message")

            #Add Tool Message (Tool Execution Results)
            safe_messages.extend(tool_messages)
            logger.debug(f"[{analyst_name}Add{len(tool_messages)}Tool Message")

            #Add Analytic Tip
            safe_messages.append(HumanMessage(content=analysis_prompt_template))
            logger.debug(f"[{analyst_name}Add analytical tips")
            
            #Record message sequence information
            total_length = sum(len(str(msg.content)) for msg in safe_messages if hasattr(msg, 'content'))
            logger.info(f"[{analyst_name}Message sequence:{len(safe_messages)}Message, total length:{total_length:,}Character")
            
            #Check if the message sequence is empty
            if not safe_messages:
                logger.error(f"[{analyst_name}The message sequence is empty to generate analysis")
                tool_summary = "\n\n".join([f"工具结果 {i+1}:\n{str(result)}" for i, result in enumerate(tool_results)])
                report = f"{analyst_name}工具调用完成，获得以下数据：\n\n{tool_summary}"
                return report, [result] + tool_messages
            
            #Generate final analysis reports
            try:
                logger.info(f"[{analyst_name}Start calling Google Model for final analysis...")
                logger.debug(f"[{analyst_name}📋LLMtype:{llm.__class__.__name__}")
                logger.debug(f"[{analyst_name}Can not open message{len(safe_messages)}")
                
                #Record the type and length of each message
                for i, msg in enumerate(safe_messages):
                    msg_type = msg.__class__.__name__
                    msg_length = len(str(msg.content)) if hasattr(msg, 'content') else 0
                    logger.debug(f"[{analyst_name}Message{i+1}: {msg_type}, length:{msg_length}")
                
                #Record the contents of analytical tips (front 200 characters)
                analysis_msg = safe_messages[-1] if safe_messages else None
                if analysis_msg and hasattr(analysis_msg, 'content'):
                    prompt_preview = str(analysis_msg.content)[:200] + "..." if len(str(analysis_msg.content)) > 200 else str(analysis_msg.content)
                    logger.debug(f"[{analyst_name}Analysis alert preview:{prompt_preview}")
                
                logger.info(f"[{analyst_name}I'm calling LLM.infoke.")
                final_result = llm.invoke(safe_messages)
                logger.info(f"[{analyst_name}✅LLM.infoke() call complete")
                
                #Check return results in detail
                logger.debug(f"[{analyst_name}Check the LLM returns...")
                logger.debug(f"[{analyst_name}Return result type: 🔍{type(final_result)}")
                logger.debug(f"[{analyst_name}Return result properties:{dir(final_result)}")
                
                if hasattr(final_result, 'content'):
                    content = final_result.content
                    logger.debug(f"[{analyst_name}Content type: 🔍{type(content)}")
                    logger.debug(f"[{analyst_name}Content length:{len(content) if content else 0}")
                    logger.debug(f"[{analyst_name}Whether or not the content is empty:{not content}")
                    
                    if content:
                        content_preview = content[:200] + "..." if len(content) > 200 else content
                        logger.debug(f"[{analyst_name}🔍Preview:{content_preview}")
                        
                        report = content
                        logger.info(f"[{analyst_name}✅Google model final analysis report produced successfully, length:{len(report)}Character")
                        
                        #Returns complete message sequence
                        all_messages = [result] + tool_messages + [final_result]
                        return report, all_messages
                    else:
                        logger.warning(f"[{analyst_name}Google model returns empty")
                        logger.debug(f"[{analyst_name}Empty content details: repr={repr(content)}")
                else:
                    logger.warning(f"[{analyst_name}⚠️Google model returns with no content properties")
                    logger.debug(f"[{analyst_name}Available Properties:{[attr for attr in dir(final_result) if not attr.startswith('_')]}")
                
                #If here, indicate empty or no substantive properties
                logger.warning(f"[{analyst_name}The final analysis of the Google model failed - the content is empty")
                #Downscaling: producing simple reports based on tool results
                tool_summary = "\n\n".join([f"工具结果 {i+1}:\n{str(result)}" for i, result in enumerate(tool_results)])
                report = f"{analyst_name}工具调用完成，获得以下数据：\n\n{tool_summary}"
                logger.info(f"[{analyst_name}Use downgrade reports, length:{len(report)}Character")
                return report, [result] + tool_messages
                
            except Exception as final_error:
                logger.error(f"[{analyst_name}The final analysis was not produced:{final_error}")
                logger.error(f"[{analyst_name}Unusual type:{type(final_error).__name__}")
                logger.error(f"[{analyst_name}Unusual details:{str(final_error)}")
                
                #Record details of abnormal stacks
                import traceback
                error_traceback = traceback.format_exc()
                logger.error(f"[{analyst_name}Unusual stack:{error_traceback}")
                
                #Downscaling: producing simple reports based on tool results
                tool_summary = "\n\n".join([f"工具结果 {i+1}:\n{str(result)}" for i, result in enumerate(tool_results)])
                report = f"{analyst_name}工具调用完成，获得以下数据：\n\n{tool_summary}"
                logger.info(f"[{analyst_name}Use downgrade reports after an anomaly, length:{len(report)}Character")
                return report, [result] + tool_messages
                
        except Exception as e:
            logger.error(f"[{analyst_name}The Google model tool has failed:{e}")
            import traceback
            traceback.print_exc()
            
            #Declining: Returning tool call information
            tool_names = [tc.get('name', 'unknown') for tc in result.tool_calls]
            report = f"{analyst_name}调用了工具 {tool_names} 但处理失败: {str(e)}"
            return report, [result]
    
    @staticmethod
    def _get_tool_name(tool):
        """Get Tool Name"""
        if hasattr(tool, 'name'):
            return tool.name
        elif hasattr(tool, '__name__'):
            return tool.__name__
        else:
            return str(tool)
    
    @staticmethod
    def _validate_tool_call(tool_call, index, analyst_name):
        """Authentication Tool Call Format"""
        try:
            if not isinstance(tool_call, dict):
                logger.warning(f"[{analyst_name}⚠️ Tool Call{index}Not dictionary format:{type(tool_call)}")
                return False
            
            #Check Required Fields
            required_fields = ['name', 'args', 'id']
            for field in required_fields:
                if field not in tool_call:
                    logger.warning(f"[{analyst_name}⚠️ Tool Call{index}Missing field '{field}': {tool_call}")
                    return False
            
            #Check Tool Name
            tool_name = tool_call.get('name')
            if not isinstance(tool_name, str) or not tool_name.strip():
                logger.warning(f"[{analyst_name}⚠️ Tool Call{index}Could not close temporary folder: %s{tool_name}")
                return False
            
            #Check Parameters
            tool_args = tool_call.get('args')
            if not isinstance(tool_args, dict):
                logger.warning(f"[{analyst_name}⚠️ Tool Call{index}argument is not a dictionary format:{type(tool_args)}")
                return False
            
            #Check ID
            tool_id = tool_call.get('id')
            if not isinstance(tool_id, str) or not tool_id.strip():
                logger.warning(f"[{analyst_name}⚠️ Tool Call{index}ID invalid:{tool_id}")
                return False
            
            logger.debug(f"[{analyst_name}✅ Tool Call{index}Authentication by:{tool_name}")
            return True
            
        except Exception as e:
            logger.error(f"[{analyst_name}❌ Tool Call{index}Authentication anomaly:{e}")
            return False
    
    @staticmethod
    def _fix_tool_call(tool_call, index, analyst_name):
        """Try to fix the tool call format"""
        try:
            logger.info(f"[{analyst_name}Try to fix the tool call{index}: {tool_call}")
            
            if not isinstance(tool_call, dict):
                logger.warning(f"[{analyst_name}❌ Unable to fix a tool call in non-dictionaries format:{type(tool_call)}")
                return None
            
            fixed_tool_call = tool_call.copy()
            
            #Fix Tool Name
            if 'name' not in fixed_tool_call or not isinstance(fixed_tool_call['name'], str):
                if 'function' in fixed_tool_call and isinstance(fixed_tool_call['function'], dict):
                    #OpenAI format conversion
                    function_data = fixed_tool_call['function']
                    if 'name' in function_data:
                        fixed_tool_call['name'] = function_data['name']
                        if 'arguments' in function_data:
                            import json
                            try:
                                if isinstance(function_data['arguments'], str):
                                    fixed_tool_call['args'] = json.loads(function_data['arguments'])
                                else:
                                    fixed_tool_call['args'] = function_data['arguments']
                            except json.JSONDecodeError:
                                fixed_tool_call['args'] = {}
                else:
                    logger.warning(f"[{analyst_name}Could not determine the name of the tool")
                    return None
            
            #Fix parameters
            if 'args' not in fixed_tool_call:
                fixed_tool_call['args'] = {}
            elif not isinstance(fixed_tool_call['args'], dict):
                try:
                    import json
                    if isinstance(fixed_tool_call['args'], str):
                        fixed_tool_call['args'] = json.loads(fixed_tool_call['args'])
                    else:
                        fixed_tool_call['args'] = {}
                except:
                    fixed_tool_call['args'] = {}
            
            #Fix ID
            if 'id' not in fixed_tool_call or not isinstance(fixed_tool_call['id'], str):
                import uuid
                fixed_tool_call['id'] = f"call_{uuid.uuid4().hex[:8]}"
            
            #Validate recovered tool calls
            if GoogleToolCallHandler._validate_tool_call(fixed_tool_call, index, analyst_name):
                logger.info(f"[{analyst_name}✅ Tool Call{index}Rehabilitation success:{fixed_tool_call['name']}")
                return fixed_tool_call
            else:
                logger.warning(f"[{analyst_name}❌ Tool Call{index}Repair Failed")
                return None
                
        except Exception as e:
            logger.error(f"[{analyst_name}❌ Tool Call{index}Fixing anomalies:{e}")
            return None
    
    @staticmethod
    def handle_simple_google_response(
        result: AIMessage,
        llm: Any,
        analyst_name: str = "分析师"
    ) -> str:
        """Process a simple Google model response (no tool to call)

Args:
result: LLM calls
llm: example of a language model
analyser's name

Returns:
str: Analytical reports
"""
        
        if not GoogleToolCallHandler.is_google_model(llm):
            return result.content
        
        logger.info(f"[{analyst_name}📝Google model direct response, length:{len(result.content)}Character")
        
        #Check content length, if processed too long
        if len(result.content) > 15000:
            logger.warning(f"[{analyst_name}The Google model is too long to be cut off...")
            return result.content[:10000] + "\n\n[注：内容已截断以确保可读性]"
        
        return result.content
    
    @staticmethod
    def generate_final_analysis_report(llm, messages: List, analyst_name: str) -> str:
        """Generate final analysis - enhanced version to support retesting and model switching

Args:
llm: LLM example
Messages: Message List
analyser's name

Returns:
str: Analytical reports
"""
        if not GoogleToolCallHandler.is_google_model(llm):
            logger.warning(f"⚠️ [{analyst_name}:: Non-Google model, skip Google tool processing Device")
            return ""
        
        #Retry Configuration
        max_retries = 3
        retry_delay = 2  #sec
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"🔍 [{analyst_name}== sync, corrected by elderman == @elder man{attempt + 1}/{max_retries}) =====")
                logger.debug(f"🔍 [{analyst_name}LM type:{type(llm).__name__}")
                logger.debug(f"🔍 [{analyst_name}LLM model:{getattr(llm, 'model', 'unknown')}")
                logger.debug(f"🔍 [{analyst_name}Number of messages:{len(messages)}")
                
                #Record message type and length
                for i, msg in enumerate(messages):
                    msg_type = type(msg).__name__
                    if hasattr(msg, 'content'):
                        content_length = len(str(msg.content)) if msg.content else 0
                        logger.debug(f"🔍 [{analyst_name}Message{i+1}: {msg_type}, length:{content_length}")
                    else:
                        logger.debug(f"🔍 [{analyst_name}Message{i+1}: {msg_type},nocontent properties")
                
                #Build Analysis Hint - Adjust to Number of Attempts
                if attempt == 0:
                    analysis_prompt = f"""
                    基于以上工具调用的结果，请为{analyst_name}生成一份详细的分析报告。
                    
                    要求：
                    1. 综合分析所有工具返回的数据
                    2. 提供清晰的投资建议和风险评估
                    3. 报告应该结构化且易于理解
                    4. 包含具体的数据支撑和分析逻辑
                    
                    请生成完整的分析报告：
                    """
                elif attempt == 1:
                    analysis_prompt = f"""
                    请简要分析{analyst_name}的工具调用结果并提供投资建议。
                    要求：简洁明了，包含关键数据和建议。
                    """
                else:
                    analysis_prompt = f"""
                    请为{analyst_name}提供一个简短的分析总结。
                    """
                
                logger.debug(f"🔍 [{analyst_name}Analysis alert preview:{analysis_prompt[:100]}...")
                
                #Optimizing message sequence
                optimized_messages = GoogleToolCallHandler._optimize_message_sequence(messages, analysis_prompt)
                
                logger.info(f"[{analyst_name}🚀 calling LLM.invoke(){attempt + 1}/{max_retries})...")
                
                #Call LLM to generate report
                import time
                start_time = time.time()
                result = llm.invoke(optimized_messages)
                end_time = time.time()
                
                logger.info(f"[{analyst_name}Call completed (time-consuming:{end_time - start_time:.2f}sec)")
                
                #Check return results in detail
                logger.debug(f"🔍 [{analyst_name}Type of return result:{type(result).__name__}")
                logger.debug(f"🔍 [{analyst_name}Return result properties:{dir(result)}")
                
                if hasattr(result, 'content'):
                    content = result.content
                    logger.debug(f"🔍 [{analyst_name}Type of content:{type(content)}")
                    logger.debug(f"🔍 [{analyst_name}Content length:{len(content) if content else 0}")
                    
                    if not content or len(content.strip()) == 0:
                        logger.warning(f"[{analyst_name}Google model returns empty{attempt + 1}/{max_retries})")
                        
                        if attempt < max_retries - 1:
                            logger.info(f"[{analyst_name}Wait{retry_delay}Try again in seconds...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            logger.warning(f"[{analyst_name}The final analysis of the Google model failed - all retests returned empty content")
                            #Use downgrade reports
                            fallback_report = GoogleToolCallHandler._generate_fallback_report(messages, analyst_name)
                            logger.info(f"[{analyst_name}Use downgrade reports, length:{len(fallback_report)}Character")
                            return fallback_report
                    else:
                        logger.info(f"[{analyst_name}Successful production of analytical reports, length:{len(content)}Character")
                        return content
                else:
                    logger.error(f"[{analyst_name}❌ returns with no content properties (tried){attempt + 1}/{max_retries})")
                    
                    if attempt < max_retries - 1:
                        logger.info(f"[{analyst_name}Wait{retry_delay}Try again in seconds...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        fallback_report = GoogleToolCallHandler._generate_fallback_report(messages, analyst_name)
                        logger.info(f"[{analyst_name}Use downgrade reports, length:{len(fallback_report)}Character")
                        return fallback_report
                        
            except Exception as e:
                logger.error(f"[{analyst_name}LLM call abnormal.{attempt + 1}/{max_retries}): {e}")
                logger.error(f"[{analyst_name}Unusual type:{type(e).__name__}")
                logger.error(f"[{analyst_name}Full anomaly:{traceback.format_exc()}")
                
                if attempt < max_retries - 1:
                    logger.info(f"[{analyst_name}Wait{retry_delay}Try again in seconds...")
                    time.sleep(retry_delay)
                    continue
                else:
                    #Use downgrade reports
                    fallback_report = GoogleToolCallHandler._generate_fallback_report(messages, analyst_name)
                    logger.info(f"[{analyst_name}Use downgrade reports, length:{len(fallback_report)}Character")
                    return fallback_report
        
        #If all retests fail, return downgrade report
        fallback_report = GoogleToolCallHandler._generate_fallback_report(messages, analyst_name)
        logger.info(f"[{analyst_name}All failed retests, using downgrade reports, length:{len(fallback_report)}Character")
        return fallback_report
    
    @staticmethod
    def _optimize_message_sequence(messages: List, analysis_prompt: str) -> List:
        """Optimize the message sequence to ensure a reasonable length Internal

Args:
Messages: Original Message List
Analysis prompt: Analytic tip

Returns:
List: Optimized list of messages
"""
        from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
        
        #Calculate total length
        total_length = sum(len(str(msg.content)) for msg in messages if hasattr(msg, 'content'))
        total_length += len(analysis_prompt)
        
        if total_length <= 50000:
            #Reasonable length, directly add analytical tips
            return messages + [HumanMessage(content=analysis_prompt)]
        
        #Need optimization: keep key messages
        optimized_messages = []
        
        #Can not open message
        for msg in messages:
            if isinstance(msg, HumanMessage):
                optimized_messages = [msg]
                break
        
        #Keep AI messages and tool messages but cut too long
        for msg in messages:
            if isinstance(msg, (AIMessage, ToolMessage)):
                if hasattr(msg, 'content') and len(str(msg.content)) > 5000:
                    #Cut Too Long
                    truncated_content = str(msg.content)[:5000] + "\n\n[注：数据已截断以确保处理效率]"
                    if isinstance(msg, AIMessage):
                        optimized_msg = AIMessage(content=truncated_content)
                    else:
                        optimized_msg = ToolMessage(
                            content=truncated_content,
                            tool_call_id=getattr(msg, 'tool_call_id', 'unknown')
                        )
                    optimized_messages.append(optimized_msg)
                else:
                    optimized_messages.append(msg)
        
        #Add Analytic Tip
        optimized_messages.append(HumanMessage(content=analysis_prompt))
        
        return optimized_messages
    
    @staticmethod
    def _generate_fallback_report(messages: List, analyst_name: str) -> str:
        """Generate downgrade reports

Args:
Messages: Message List
analyser's name

Returns:
str: Deduction report
"""
        from langchain_core.messages import ToolMessage
        
        #Extract Tool Results
        tool_results = []
        for msg in messages:
            if isinstance(msg, ToolMessage) and hasattr(msg, 'content'):
                content = str(msg.content)
                if len(content) > 1000:
                    content = content[:1000] + "\n\n[注：数据已截断]"
                tool_results.append(content)
        
        if tool_results:
            tool_summary = "\n\n".join([f"工具结果 {i+1}:\n{result}" for i, result in enumerate(tool_results)])
            report = f"{analyst_name}工具调用完成，获得以下数据：\n\n{tool_summary}\n\n注：由于模型响应异常，此为基于工具数据的简化报告。"
        else:
            report = f"{analyst_name}分析完成，但未能获取到有效的工具数据。建议检查数据源或重新尝试分析。"
        
        return report
    
    @staticmethod
    def create_analysis_prompt(
        ticker: str,
        company_name: str,
        analyst_type: str,
        specific_requirements: str = ""
    ) -> str:
        """Create standard analytical tips

Args:
ticker: Stock code
Company name: Company name
Analyst type: Analyst type of analyst (e.g. "Technology" and "Basic Analysis" etc.)
Special requirements:

Returns:
st: Analyze hints
"""
        
        base_prompt = f"""现在请基于上述工具获取的数据，生成详细的{analyst_type}报告。

**股票信息：**
- 公司名称：{company_name}
- 股票代码：{ticker}

**分析要求：**
1. 报告必须基于工具返回的真实数据进行分析
2. 包含具体的数值和专业分析
3. 提供明确的投资建议和风险提示
4. 报告长度不少于800字
5. 使用中文撰写
6. 确保在分析中正确使用公司名称"{company_name}"和股票代码"{ticker}"

{specific_requirements}

请生成专业、详细的{analyst_type}报告。"""
        
        return base_prompt