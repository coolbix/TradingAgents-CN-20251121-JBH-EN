"""Enhancing News Filters - Costed Small Models and Rules Filtering
Support multiple filtering strategies: rule filtering, semantic similarity, local classification models
"""

import pandas as pd
import re
import logging
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import numpy as np

#Import Basic Filter
from .news_filter import NewsRelevanceFilter, create_news_filter, get_company_name

logger = logging.getLogger(__name__)

class EnhancedNewsFilter(NewsRelevanceFilter):
    """Enhanced news filters, cost-based models and multiple filtering strategies"""
    
    def __init__(self, stock_code: str, company_name: str, use_semantic: bool = True, use_local_model: bool = False):
        """Initialize Enhanced Filter

Args:
Stock code: Stock code
Company name: Company name
use semantic: whether semantic similarity is used to filter
use local model: using local classification models
"""
        super().__init__(stock_code, company_name)
        self.use_semantic = use_semantic
        self.use_local_model = use_local_model
        
        #Semantic Model Relevant
        self.sentence_model = None
        self.company_embedding = None
        
        #Local Classification Model Relevant
        self.classification_model = None
        self.tokenizer = None
        
        #Initialization Model
        if use_semantic:
            self._init_semantic_model()
        if use_local_model:
            self._init_classification_model()
    
    def _init_semantic_model(self):
        """Initialise semantic similarity model"""
        try:
            logger.info("[enhanced filter] Loading semantic similarity models...")
            
            #Try using events-transformers
            try:
                from sentence_transformers import SentenceTransformer
                
                #Use lightweight Chinese model
                model_name = "paraphrase-multilingual-MiniLM-L12-v2"  #Lightweight model supporting Chinese
                self.sentence_model = SentenceTransformer(model_name)
                
                #Expected company-related embedding
                company_texts = [
                    self.company_name,
                    f"{self.company_name}股票",
                    f"{self.company_name}公司",
                    f"{self.stock_code}",
                    f"{self.company_name}业绩",
                    f"{self.company_name}财报"
                ]
                
                self.company_embedding = self.sentence_model.encode(company_texts)
                logger.info(f"[enhanced filter] ✅ semantic model loaded successfully:{model_name}")
                
            except ImportError:
                logger.warning("[Enhanced filter] sentence-transformers are not installed, skip semantic filtering")
                self.use_semantic = False
                
        except Exception as e:
            logger.error(f"[Enhanced filter] semantic model initialization failed:{e}")
            self.use_semantic = False
    
    def _init_classification_model(self):
        """Initialize local classification models"""
        try:
            logger.info("[enhanced filter] Loading local classification models...")
            
            #Try the Chinese classification model using the Transformers library
            try:
                from transformers import AutoTokenizer, AutoModelForSequenceClassification
                import torch
                
                #Use a lightweight Chinese text classification model
                model_name = "uer/roberta-base-finetuned-chinanews-chinese"
                
                self.tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.classification_model = AutoModelForSequenceClassification.from_pretrained(model_name)
                
                logger.info(f"[enhanced filter] ✅ classification model loaded successfully:{model_name}")
                
            except ImportError:
                logger.warning("[Enhanced Filter] Transformers are not installed, skip local model categories")
                self.use_local_model = False
                
        except Exception as e:
            logger.error(f"[Enhanced filter] Initialization of local classification model failed:{e}")
            self.use_local_model = False
    
    def calculate_semantic_similarity(self, title: str, content: str) -> float:
        """Calculate semantic similarity rating

Args:
title:
Content:

Returns:
float: Semantic Similarity Rating (0-100)
"""
        if not self.use_semantic or self.sentence_model is None:
            return 0
        
        try:
            #Pre-200 characters for combining titles and contents
            text = f"{title} {content[:200]}"
            
            #Compute textembeding
            text_embedding = self.sentence_model.encode([text])
            
            #Compute similarity to company-related text
            similarities = []
            for company_emb in self.company_embedding:
                similarity = np.dot(text_embedding[0], company_emb) / (
                    np.linalg.norm(text_embedding[0]) * np.linalg.norm(company_emb)
                )
                similarities.append(similarity)
            
            #Take Maximum Similarity
            max_similarity = max(similarities)
            
            #Convert to 0-100 points
            semantic_score = max(0, min(100, max_similarity * 100))
            
            logger.debug(f"[Enhanced filter] Semantic similarity rating:{semantic_score:.1f}")
            return semantic_score
            
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return 0
    
    def classify_news_relevance(self, title: str, content: str) -> float:
        """Use local models to classify news relevance

Args:
title:
Content:

Returns:
float: classification relevance rating (0-100)
"""
        if not self.use_local_model or self.classification_model is None:
            return 0
        
        try:
            import torch
            
            #Build classification text
            text = f"{title} {content[:300]}"
            
            #Add Corporate Information as Context
            context_text = f"关于{self.company_name}({self.stock_code})的新闻: {text}"
            
            #Phrases and coding
            inputs = self.tokenizer(
                context_text,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512
            )
            
            #Model reasoning
            with torch.no_grad():
                outputs = self.classification_model(**inputs)
                logits = outputs.logits
                
                #Use softmax to get probability distribution
                probabilities = torch.softmax(logits, dim=-1)
                
                #Assuming the first category is "relevant," the second is "not relevant."
                #It needs to be adapted to specific models.
                relevance_prob = probabilities[0][0].item()  #Probability of relevance
                
                #Convert to 0-100 points
                classification_score = relevance_prob * 100
                
                logger.debug(f"[Enhanced filter] Classification model rating:{classification_score:.1f}")
                return classification_score
                
        except Exception as e:
            logger.error(f"[Enhanced filter] Local model classification failed:{e}")
            return 0
    
    def calculate_enhanced_relevance_score(self, title: str, content: str) -> Dict[str, float]:
        """Calculation of enhanced relevance ratings (integrated multiple methods)

Args:
title:
Content:

Returns:
Dict: Dictionary with various ratings
"""
        scores = {}
        
        #1. Basic rule scoring
        rule_score = super().calculate_relevance_score(title, content)
        scores['rule_score'] = rule_score
        
        #Semantic similarity rating
        if self.use_semantic:
            semantic_score = self.calculate_semantic_similarity(title, content)
            scores['semantic_score'] = semantic_score
        else:
            scores['semantic_score'] = 0
        
        #Local model classification ratings
        if self.use_local_model:
            classification_score = self.classify_news_relevance(title, content)
            scores['classification_score'] = classification_score
        else:
            scores['classification_score'] = 0
        
        #4. Comprehensive rating (weighted average)
        weights = {
            'rule': 0.4,      #Rule filter weight 40%
            'semantic': 0.35,  #Semantic similarity weight 35%
            'classification': 0.25  #Classification model weight 25%
        }
        
        final_score = (
            weights['rule'] * rule_score +
            weights['semantic'] * scores['semantic_score'] +
            weights['classification'] * scores['classification_score']
        )
        
        scores['final_score'] = final_score
        
        logger.debug(f"[enhanced filter] Comprehensive scoring - Rules:{rule_score:.1f}, semantic:{scores['semantic_score']:.1f}, "
                    f"Classification:{scores['classification_score']:.1f}, eventually:{final_score:.1f}")
        
        return scores
    
    def filter_news_enhanced(self, news_df: pd.DataFrame, min_score: float = 40) -> pd.DataFrame:
        """Enhance news filtering

Args:
News df: RawDataFrame
min score: Minimum integrated rating threshold

Returns:
pd. DataFrame: Filtered NewsDataFrame with detailed rating information
"""
        if news_df.empty:
            logger.warning("[enhanced filter] Enter NewsDataFrame empty")
            return news_df
        
        logger.info(f"[enhanced filter] Start enhancing filter, original number:{len(news_df)}Article, lowest rating threshold:{min_score}")
        
        filtered_news = []
        
        for idx, row in news_df.iterrows():
            title = row.get('新闻标题', row.get('标题', ''))
            content = row.get('新闻内容', row.get('内容', ''))
            
            #Calculate enhanced rating
            scores = self.calculate_enhanced_relevance_score(title, content)
            
            if scores['final_score'] >= min_score:
                row_dict = row.to_dict()
                row_dict.update(scores)  #Add all rating information
                filtered_news.append(row_dict)
                
                logger.debug(f"[enhanced filter] Keep the news.{scores['final_score']:.1f}): {title[:50]}...")
            else:
                logger.debug(f"[enhanced filter] Filter news.{scores['final_score']:.1f}): {title[:50]}...")
        
        #Create filtered DataFrame
        if filtered_news:
            filtered_df = pd.DataFrame(filtered_news)
            #Sort by comprehensive rating
            filtered_df = filtered_df.sort_values('final_score', ascending=False)
            logger.info(f"[enhanced filter] Enhance filter completion, retain{len(filtered_df)}Public information")
        else:
            filtered_df = pd.DataFrame()
            logger.warning(f"[enhanced filters] All news is filtered, no qualified news.")
            
        return filtered_df


def create_enhanced_news_filter(ticker: str, use_semantic: bool = True, use_local_model: bool = False) -> EnhancedNewsFilter:
    """Create a convenient function to enhance the news filter

Args:
ticker: Stock code
use semantic: whether semantic similarity is used to filter
use local model: using local classification models

Returns:
Enhanced NewsFilter: configured examples of enhanced filters
"""
    company_name = get_company_name(ticker)
    return EnhancedNewsFilter(ticker, company_name, use_semantic, use_local_model)


#Use Example
if __name__ == "__main__":
    #Test Enhancement Filter
    import pandas as pd
    
    #Simulation of news data
    test_news = pd.DataFrame([
        {
            '新闻标题': '招商银行发布2024年第三季度业绩报告',
            '新闻内容': '招商银行今日发布第三季度财报，净利润同比增长8%，资产质量持续改善...'
        },
        {
            '新闻标题': '上证180ETF指数基金（530280）自带杠铃策略',
            '新闻内容': '数据显示，上证180指数前十大权重股分别为贵州茅台、招商银行600036...'
        },
        {
            '新闻标题': '银行ETF指数(512730)多只成分股上涨',
            '新闻内容': '银行板块今日表现强势，招商银行、工商银行等多只成分股上涨...'
        },
        {
            '新闻标题': '招商银行与某科技公司签署战略合作协议',
            '新闻内容': '招商银行宣布与知名科技公司达成战略合作，将在数字化转型方面深度合作...'
        }
    ])
    
    print("=== 测试增强新闻过滤器 ===")
    
    #Create enhanced filters (only rule filters to avoid model dependence)
    enhanced_filter = create_enhanced_news_filter('600036', use_semantic=False, use_local_model=False)
    
    #Filter News
    filtered_news = enhanced_filter.filter_news_enhanced(test_news, min_score=30)
    
    print(f"原始新闻: {len(test_news)}条")
    print(f"过滤后新闻: {len(filtered_news)}条")
    
    if not filtered_news.empty:
        print("\n过滤后的新闻:")
        for _, row in filtered_news.iterrows():
            print(f"- {row['新闻标题']} (综合评分: {row['final_score']:.1f})")
            print(f"  规则评分: {row['rule_score']:.1f}, 语义评分: {row['semantic_score']:.1f}, 分类评分: {row['classification_score']:.1f}")