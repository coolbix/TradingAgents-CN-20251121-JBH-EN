# TradingAgents/graph/signal_processing.py

from langchain_openai import ChatOpenAI

#Import unified log system and chart processing module log decorations
from tradingagents.utils.logging_init import get_logger
from tradingagents.utils.tool_logging import log_graph_module
logger = get_logger("graph.signal_processing")


class SignalProcessor:
    """Processes trading signals to extract actionable decisions."""

    def __init__(self, quick_thinking_llm: ChatOpenAI):
        """Initialize with an LLM for processing."""
        self.quick_thinking_llm = quick_thinking_llm

    @log_graph_module("signal_processing")
    def process_signal(self, full_signal: str, stock_symbol: str = None) -> dict:
        """
        Process a full trading signal to extract structured decision information.

        Args:
            full_signal: Complete trading signal text
            stock_symbol: Stock symbol to determine currency type

        Returns:
            Dictionary containing extracted decision information
        """

        #Validate input parameters
        if not full_signal or not isinstance(full_signal, str) or len(full_signal.strip()) == 0:
            logger.error(f"[SignalProcessor] Input signal is empty or invalid:{repr(full_signal)}")
            return {
                'action': '持有',
                'target_price': None,
                'confidence': 0.5,
                'risk_score': 0.5,
                'reasoning': '输入信号无效，默认持有建议'
            }

        #Clear and validate signal contents
        full_signal = full_signal.strip()
        if len(full_signal) == 0:
            logger.error(f"[SignalProcessor]")
            return {
                'action': '持有',
                'target_price': None,
                'confidence': 0.5,
                'risk_score': 0.5,
                'reasoning': '信号内容为空，默认持有建议'
            }

        #Test stock type and currency
        from tradingagents.utils.stock_utils import StockUtils

        market_info = StockUtils.get_market_info(stock_symbol)
        is_china = market_info['is_china']
        is_hk = market_info['is_hk']
        currency = market_info['currency_name']
        currency_symbol = market_info['currency_symbol']

        logger.info(f"[SignalProcessor]{stock_symbol}market ={market_info['market_name']}currency ={currency}",
                   extra={'stock_symbol': stock_symbol, 'market': market_info['market_name'], 'currency': currency})

        messages = [
            (
                "system",
                f"""您是一位专业的金融分析助手，负责从交易员的分析报告中提取结构化的投资决策信息。

请从提供的分析报告中提取以下信息，并以JSON格式返回：

{{
    "action": "买入/持有/卖出",
    "target_price": 数字({currency}价格，**必须提供具体数值，不能为null**),
    "confidence": 数字(0-1之间，如果没有明确提及则为0.7),
    "risk_score": 数字(0-1之间，如果没有明确提及则为0.5),
    "reasoning": "决策的主要理由摘要"
}}

请确保：
1. action字段必须是"买入"、"持有"或"卖出"之一（绝对不允许使用英文buy/hold/sell）
2. target_price必须是具体的数字,target_price应该是合理的{currency}价格数字（使用{currency_symbol}符号）
3. confidence和risk_score应该在0-1之间
4. reasoning应该是简洁的中文摘要
5. 所有内容必须使用中文，不允许任何英文投资建议

特别注意：
- 股票代码 {stock_symbol or '未知'} 是{market_info['market_name']}，使用{currency}计价
- 目标价格必须与股票的交易货币一致（{currency_symbol}）

如果某些信息在报告中没有明确提及，请使用合理的默认值。""",
            ),
            ("human", full_signal),
        ]

        #Verify message contents
        if not messages or len(messages) == 0:
            logger.error(f"[SignalProcessor] messages are empty.")
            return self._get_default_decision()
        
        #Could not close temporary folder: %s
        human_content = messages[1][1] if len(messages) > 1 else ""
        if not human_content or len(human_content.strip()) == 0:
            logger.error(f"[SignalProcessor] human message is empty.")
            return self._get_default_decision()

        logger.debug(f"[SignalProcessor]{len(messages)}, signal length:{len(full_signal)}")

        try:
            response = self.quick_thinking_llm.invoke(messages).content
            logger.debug(f"[SignalProcessor] LLM responded:{response[:200]}...")

            #Try to parse JSON's response
            import json
            import re

            #Extract JSON section
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_text = json_match.group()
                logger.debug(f"[SignalProcessor]{json_text}")
                decision_data = json.loads(json_text)

                #Validation and standardization of data
                action = decision_data.get('action', '持有')
                if action not in ['买入', '持有', '卖出']:
                    #Try mapping English and other variables
                    action_map = {
                        'buy': '买入', 'hold': '持有', 'sell': '卖出',
                        'BUY': '买入', 'HOLD': '持有', 'SELL': '卖出',
                        '购买': '买入', '保持': '持有', '出售': '卖出',
                        'purchase': '买入', 'keep': '持有', 'dispose': '卖出'
                    }
                    action = action_map.get(action, '持有')
                    if action != decision_data.get('action', '持有'):
                        logger.debug(f"[SignalProcessor]{decision_data.get('action')} -> {action}")

                #Process target prices to ensure correct extraction
                target_price = decision_data.get('target_price')
                if target_price is None or target_price == "null" or target_price == "":
                    #If JSON does not have a target price, try to extract it from the whole text
                    reasoning = decision_data.get('reasoning', '')
                    full_text = f"{reasoning} {full_signal}"  #Expand Search
                    
                    #Enhanced price matching mode
                    price_patterns = [
                        r'目标价[位格]?[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',  #Target price: 45.50
                        r'目标[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',         #Objective: 45.50
                        r'价格[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',         #Price: 45.50
                        r'价位[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',         #Price: 45.50
                        r'合理[价位格]?[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)', #Reasonable price: 45.50
                        r'估值[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',         #Valuation: 45.50
                        r'[¥\$](\d+(?:\.\d+)?)',                      #45.50 or $190
                        r'(\d+(?:\.\d+)?)元',                         #45.50.
                        r'(\d+(?:\.\d+)?)美元',                       #$190.
                        r'建议[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',        #Recommendation: 45.50
                        r'预期[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',        #Expected: 45.50
                        r'看[到至]\s*[¥\$]?(\d+(?:\.\d+)?)',          #Seeing 45.50.
                        r'上涨[到至]\s*[¥\$]?(\d+(?:\.\d+)?)',        #Up to 45.50.
                        r'(\d+(?:\.\d+)?)\s*[¥\$]',                  # 45.50¥
                    ]
                    
                    for pattern in price_patterns:
                        price_match = re.search(pattern, full_text, re.IGNORECASE)
                        if price_match:
                            try:
                                target_price = float(price_match.group(1))
                                logger.debug(f"[SignalProcessor]{target_price}(Model:{pattern})")
                                break
                            except (ValueError, IndexError):
                                continue

                    #If you still haven't found a price, try to figure it out.
                    if target_price is None or target_price == "null" or target_price == "":
                        target_price = self._smart_price_estimation(full_text, action, is_china)
                        if target_price:
                            logger.debug(f"[SignalProcessor]{target_price}")
                        else:
                            target_price = None
                            logger.warning(f"[SignalProcessor] failed to extract the target price, set to None")
                else:
                    #Ensure price is a numerical type
                    try:
                        if isinstance(target_price, str):
                            #Clear the price for string format
                            clean_price = target_price.replace('$', '').replace('¥', '').replace('￥', '').replace('元', '').replace('美元', '').strip()
                            target_price = float(clean_price) if clean_price and clean_price.lower() not in ['none', 'null', ''] else None
                        elif isinstance(target_price, (int, float)):
                            target_price = float(target_price)
                        logger.debug(f"[SignalProcessor]{target_price}")
                    except (ValueError, TypeError):
                        target_price = None
                        logger.warning(f"[SignalProcessor] Price conversion failed, set to None")

                result = {
                    'action': action,
                    'target_price': target_price,
                    'confidence': float(decision_data.get('confidence', 0.7)),
                    'risk_score': float(decision_data.get('risk_score', 0.5)),
                    'reasoning': decision_data.get('reasoning', '基于综合分析的投资建议')
                }
                logger.info(f"[SignalProcessor]{result}",
                           extra={'action': result['action'], 'target_price': result['target_price'],
                                 'confidence': result['confidence'], 'stock_symbol': stock_symbol})
                return result
            else:
                #If JSON cannot be parsed, extract with simple text
                return self._extract_simple_decision(response)

        except Exception as e:
            logger.error(f"Signal processing error:{e}", exc_info=True, extra={'stock_symbol': stock_symbol})
            #Back to simple extraction
            return self._extract_simple_decision(full_signal)

    def _smart_price_estimation(self, text: str, action: str, is_china: bool) -> float:
        """Smart price extrapolation method"""
        import re
        
        #Try to extract current price and drop information from text
        current_price = None
        percentage_change = None
        
        #Extract Current Price
        current_price_patterns = [
            r'当前价[格位]?[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',
            r'现价[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',
            r'股价[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',
            r'价格[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',
        ]
        
        for pattern in current_price_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    current_price = float(match.group(1))
                    break
                except ValueError:
                    continue
        
        #Can not open message
        percentage_patterns = [
            r'上涨\s*(\d+(?:\.\d+)?)%',
            r'涨幅\s*(\d+(?:\.\d+)?)%',
            r'增长\s*(\d+(?:\.\d+)?)%',
            r'(\d+(?:\.\d+)?)%\s*的?上涨',
        ]
        
        for pattern in percentage_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    percentage_change = float(match.group(1)) / 100
                    break
                except ValueError:
                    continue
        
        #Calculate target price based on actions and information
        if current_price and percentage_change:
            if action == '买入':
                return round(current_price * (1 + percentage_change), 2)
            elif action == '卖出':
                return round(current_price * (1 - percentage_change), 2)
        
        #Use default estimation if current prices do not rise or fall
        if current_price:
            if action == '买入':
                #Buy recommendation default 10-20% increase
                multiplier = 1.15 if is_china else 1.12
                return round(current_price * multiplier, 2)
            elif action == '卖出':
                #Sold proposal defaults 5-10% down Fan
                multiplier = 0.95 if is_china else 0.92
                return round(current_price * multiplier, 2)
            else:  #Hold
                #Holding recommended use of current prices
                return current_price
        
        return None

    def _extract_simple_decision(self, text: str) -> dict:
        """Simple decision extraction as backup"""
        import re

        #Rip Action
        action = '持有'  #Default
        if re.search(r'买入|BUY', text, re.IGNORECASE):
            action = '买入'
        elif re.search(r'卖出|SELL', text, re.IGNORECASE):
            action = '卖出'
        elif re.search(r'持有|HOLD', text, re.IGNORECASE):
            action = '持有'

        #Attempt to extract target prices (using enhanced models)
        target_price = None
        price_patterns = [
            r'目标价[位格]?[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',  #Target price: 45.50
            r'\*\*目标价[位格]?\*\*[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',  #** Target price**: 45.50
            r'目标[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',         #Objective: 45.50
            r'价格[：:]?\s*[¥\$]?(\d+(?:\.\d+)?)',         #Price: 45.50
            r'[¥\$](\d+(?:\.\d+)?)',                      #45.50 or $190
            r'(\d+(?:\.\d+)?)元',                         #45.50.
        ]

        for pattern in price_patterns:
            price_match = re.search(pattern, text)
            if price_match:
                try:
                    target_price = float(price_match.group(1))
                    break
                except ValueError:
                    continue

        #If you don't find a price, try smart calculations.
        if target_price is None:
            #Test for stock type
            is_china = True  #The default assumption is Unit A and should actually be obtained from context
            target_price = self._smart_price_estimation(text, action, is_china)

        return {
            'action': action,
            'target_price': target_price,
            'confidence': 0.7,
            'risk_score': 0.5,
            'reasoning': '基于综合分析的投资建议'
        }

    def _get_default_decision(self) -> dict:
        """Return to default investment decision"""
        return {
            'action': '持有',
            'target_price': None,
            'confidence': 0.5,
            'risk_score': 0.5,
            'reasoning': '输入数据无效，默认持有建议'
        }
