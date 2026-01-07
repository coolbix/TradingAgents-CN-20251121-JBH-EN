import asyncio
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import get_mongo_db_async, init_database_async

async def main():
    await init_database_async()
    db = get_mongo_db_async()
    
    # 取一条记录看看完整结构
    print("🔍 查看 stock_financial_data 集合文档结构...\n")
    
    doc = await db['stock_financial_data'].find_one()
    if doc:
        # 移除 _id 字段
        doc.pop('_id', None)
        print(json.dumps(doc, indent=2, default=str, ensure_ascii=False))
    else:
        print("❌ 集合为空")

if __name__ == '__main__':
    asyncio.run(main())

