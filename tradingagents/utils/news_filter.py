"""News Relevance Filter
For filtering news that is not relevant to a particular stock/company to improve the quality of news analysis
"""

import pandas as pd
import re
from typing import List, Dict, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class NewsRelevanceFilter:
    """Rules-based news relevance filter"""
    
    def __init__(self, stock_code: str, company_name: str):
        """Initializing Filter

Args:
Stock code: Stock code, e. g. "600036"
Company name: Name of the company, e.g. "Rendering Bank"
"""
        self.stock_code = stock_code.upper()
        self.company_name = company_name
        
        #Exclude keywords - lower relevance when these words appear
        self.exclude_keywords = [
            'etf', '指数基金', '基金', '指数', 'index', 'fund',
            '权重股', '成分股', '板块', '概念股', '主题基金',
            '跟踪指数', '被动投资', '指数投资', '基金持仓'
        ]
        
        #Include keywords - they increase relevance when they appear
        self.include_keywords = [
            '业绩', '财报', '公告', '重组', '并购', '分红', '派息',
            '高管', '董事', '股东', '增持', '减持', '回购',
            '年报', '季报', '半年报', '业绩预告', '业绩快报',
            '股东大会', '董事会', '监事会', '重大合同',
            '投资', '收购', '出售', '转让', '合作', '协议'
        ]
        
        #Strongly relevant keywords - significant increase in relevance when they appear
        self.strong_keywords = [
            '停牌', '复牌', '涨停', '跌停', '限售解禁',
            '股权激励', '员工持股', '定增', '配股', '送股',
            '资产重组', '借壳上市', '退市', '摘帽', 'ST'
        ]
    
    def calculate_relevance_score(self, title: str, content: str) -> float:
        """Calculate news relevance rating

Args:
title:
Content:

Returns:
float: Relevance rating (0-100)
"""
        score = 0
        title_lower = title.lower()
        content_lower = content.lower()
        
        #1. Direct reference to company name
        if self.company_name in title:
            score += 50  #The name of the company appears in the title. High score.
            logger.debug(f"[filter] Title contains company name '{self.company_name}': +50 minutes")
        elif self.company_name in content:
            score += 25  #The name of the company appears in the content, medium
            logger.debug(f"[filter] Content contains company name '{self.company_name}': +25 minutes")
            
        #Direct reference to stock codes
        if self.stock_code in title:
            score += 40  #Stock code in the title. High score.
            logger.debug(f"[filter] Title contains stock code '{self.stock_code}': +40 minutes")
        elif self.stock_code in content:
            score += 20  #Stock code in content, medium
            logger.debug(f"[Filter] Content contains stock code '{self.stock_code}': 20 cents")
            
        #3. Strongly relevant keyword checks
        strong_matches = []
        for keyword in self.strong_keywords:
            if keyword in title_lower:
                score += 30
                strong_matches.append(keyword)
            elif keyword in content_lower:
                score += 15
                strong_matches.append(keyword)
        
        if strong_matches:
            logger.debug(f"[filter] Strongly relevant keyword match:{strong_matches}")
            
        #Include keyword checks
        include_matches = []
        for keyword in self.include_keywords:
            if keyword in title_lower:
                score += 15
                include_matches.append(keyword)
            elif keyword in content_lower:
                score += 8
                include_matches.append(keyword)
        
        if include_matches:
            logger.debug(f"[Filter] Relevant keyword match:{include_matches[:3]}...")  #Show only first three
            
        #5. Exclusion of keyword checks (minus)
        exclude_matches = []
        for keyword in self.exclude_keywords:
            if keyword in title_lower:
                score -= 40  #There's an exclusion in the title, and there's a substantial reduction.
                exclude_matches.append(keyword)
            elif keyword in content_lower:
                score -= 20  #There's an exclusionary word in the contents, medium reduction.
                exclude_matches.append(keyword)
        
        if exclude_matches:
            logger.debug(f"[filter] Excludes keyword matching:{exclude_matches[:3]}...")
            
        #6. Special rules: severe deductions if the title does not contain company information at all but contains exclusions
        if (self.company_name not in title and self.stock_code not in title and 
            any(keyword in title_lower for keyword in self.exclude_keywords)):
            score -= 30
            logger.debug(f"[Filter] Title without corporate information but with exclusionary words: -30 min")
        
        #Ensure rating in the 0-100 range Internal
        final_score = max(0, min(100, score))
        
        logger.debug(f"[Filter] Final rating:{final_score}Division - Title:{title[:30]}...")
        
        return final_score
    
    def filter_news(self, news_df: pd.DataFrame, min_score: float = 30) -> pd.DataFrame:
        """Filter NewsDataFrame

Args:
News df: RawDataFrame
min score: Minimum relevance rating threshold

Returns:
pd. DataFrame: Filtered NewsDataFrame, in order of relevance
"""
        if news_df.empty:
            logger.warning("[Filter] Enter NewsDataFrame empty")
            return news_df
        
        logger.info(f"[filter ] Start filtering news, original number:{len(news_df)}Article, lowest rating threshold:{min_score}")
        
        filtered_news = []
        
        for idx, row in news_df.iterrows():
            title = row.get('新闻标题', row.get('标题', ''))
            content = row.get('新闻内容', row.get('内容', ''))
            
            #Calculate relevance rating
            score = self.calculate_relevance_score(title, content)
            
            if score >= min_score:
                row_dict = row.to_dict()
                row_dict['relevance_score'] = score
                filtered_news.append(row_dict)
                
                logger.debug(f"[Filter] Keep the news.{score:.1f}): {title[:50]}...")
            else:
                logger.debug(f"[Filter] Filter News{score:.1f}): {title[:50]}...")
        
        #Create filtered DataFrame
        if filtered_news:
            filtered_df = pd.DataFrame(filtered_news)
            #Sort by relevance
            filtered_df = filtered_df.sort_values('relevance_score', ascending=False)
            logger.info(f"[filter] Filter complete, keep{len(filtered_df)}Public information")
        else:
            filtered_df = pd.DataFrame()
            logger.warning(f"[Filter] All news is filtered, no qualified news.")
            
        return filtered_df
    
    def get_filter_statistics(self, original_df: pd.DataFrame, filtered_df: pd.DataFrame) -> Dict:
        """Fetch filter statistical information

Args:
Original df: Raw DataFrame
filtered df: Post-FilterDataFrame

Returns:
Dict: Statistical information
"""
        stats = {
            'original_count': len(original_df),
            'filtered_count': len(filtered_df),
            'filter_rate': (len(original_df) - len(filtered_df)) / len(original_df) * 100 if len(original_df) > 0 else 0,
            'avg_score': filtered_df['relevance_score'].mean() if not filtered_df.empty else 0,
            'max_score': filtered_df['relevance_score'].max() if not filtered_df.empty else 0,
            'min_score': filtered_df['relevance_score'].min() if not filtered_df.empty else 0
        }
        
        return stats


#Map of stock code to company name
STOCK_COMPANY_MAPPING = {
    #Major Bank, Unit A
    '600036': '招商银行',
    '000001': '平安银行', 
    '600000': '浦发银行',
    '601166': '兴业银行',
    '002142': '宁波银行',
    '601328': '交通银行',
    '601398': '工商银行',
    '601939': '建设银行',
    '601288': '农业银行',
    '601818': '光大银行',
    '600015': '华夏银行',
    '600016': '民生银行',
    
    #Main White Wine Unit A
    '000858': '五粮液',
    '600519': '贵州茅台',
    '000568': '泸州老窖',
    '002304': '洋河股份',
    '000596': '古井贡酒',
    '603369': '今世缘',
    '000799': '酒鬼酒',
    
    #Major Science and Technology Unit, Unit A
    '000002': '万科A',
    '000858': '五粮液',
    '002415': '海康威视',
    '000725': '京东方A',
    '002230': '科大讯飞',
    '300059': '东方财富',
    
    #More stocks can continue to add...
}

def get_company_name(ticker: str) -> str:
    """The name of the company to which the stock code corresponds

Args:
ticker: Stock code

Returns:
str: Company name
"""
    #Clear stock code (delegate suffix)
    clean_ticker = ticker.split('.')[0]
    
    company_name = STOCK_COMPANY_MAPPING.get(clean_ticker)
    
    if company_name:
        logger.debug(f"[corporate mapping]{ticker} -> {company_name}")
        return company_name
    else:
        #If no map, return the default name
        default_name = f"股票{clean_ticker}"
        logger.warning(f"[corporate map] Not found{ticker}, by default:{default_name}")
        return default_name


def create_news_filter(ticker: str) -> NewsRelevanceFilter:
    """A convenient function to create a news filter

Args:
ticker: Stock code

Returns:
NewsRelevanceFilter: Examples of configured filters
"""
    company_name = get_company_name(ticker)
    return NewsRelevanceFilter(ticker, company_name)


#Use Example
if __name__ == "__main__":
    #Test Filter
    import pandas as pd
    
    #Simulation of news data
    test_news = pd.DataFrame([
        {
            '新闻标题': '招商银行发布2024年第三季度业绩报告',
            '新闻内容': '招商银行今日发布第三季度财报，净利润同比增长8%...'
        },
        {
            '新闻标题': '上证180ETF指数基金（530280）自带杠铃策略',
            '新闻内容': '数据显示，上证180指数前十大权重股分别为贵州茅台、招商银行600036...'
        },
        {
            '新闻标题': '银行ETF指数(512730多只成分股上涨',
            '新闻内容': '银行板块今日表现强势，招商银行、工商银行等多只成分股上涨...'
        }
    ])
    
    #Create Filter
    filter = create_news_filter('600036')
    
    #Filter News
    filtered_news = filter.filter_news(test_news, min_score=30)
    
    print(f"原始新闻: {len(test_news)}条")
    print(f"过滤后新闻: {len(filtered_news)}条")
    
    if not filtered_news.empty:
        print("\n过滤后的新闻:")
        for _, row in filtered_news.iterrows():
            print(f"- {row['新闻标题']} (评分: {row['relevance_score']:.1f})")