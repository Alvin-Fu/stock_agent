#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公司股票代码验证工具
使用Tushare的stock_basic数据来验证和匹配公司名称与股票代码
"""
import pandas as pd
import re
from utils.logger import logger
from tools.stock.tushare_fetcher import TushareFetcher


class CompanyCodeValidator:
    """公司股票代码验证器"""
    
    def __init__(self):
        self.fetcher = TushareFetcher()
        self.stock_basic_df = None
        self._load_stock_basic()
    
    def _load_stock_basic(self):
        """加载股票基础数据"""
        try:
            logger.info("📊 正在加载股票基础信息...")
            self.stock_basic_df = self.fetcher.get_stock_basic()
            
            if self.stock_basic_df is not None and not self.stock_basic_df.empty:
                # 清理数据
                self.stock_basic_df = self.stock_basic_df.dropna(subset=['name'])
                self.stock_basic_df['name'] = self.stock_basic_df['name'].apply(lambda x: str(x).strip())
                logger.info(f"✅ 成功加载 {len(self.stock_basic_df)} 只股票的基础信息")
            else:
                logger.warning("⚠️ 股票基础信息为空，代码验证功能可能受限")
        except Exception as e:
            logger.error(f"❌ 加载股票基础信息失败: {e}")
            self.stock_basic_df = pd.DataFrame()
    
    def find_stock_code(self, company_name: str) -> str:
        """
        根据公司名称查找股票代码
        
        Args:
            company_name: 公司名称
            
        Returns:
            找到的股票代码（6位数字），找不到返回None
        """
        if not company_name or self.stock_basic_df.empty:
            return None
        
        company_name = str(company_name).strip()
        
        # 1. 精确匹配
        match = self.stock_basic_df[self.stock_basic_df['name'] == company_name]
        if not match.empty:
            # 优先返回A股（没有后缀的）
            for _, row in match.iterrows():
                ts_code = row['ts_code']
                # 去掉后缀，只返回6位数字
                code = ts_code.split('.')[0]
                if re.match(r'^\d{6}$', code):
                    logger.info(f"✅ 精确匹配: {company_name} -> {code}")
                    return code
        
        # 2. 包含匹配（公司名包含在股票名中）
        match = self.stock_basic_df[self.stock_basic_df['name'].str.contains(company_name, na=False)]
        if not match.empty:
            for _, row in match.iterrows():
                ts_code = row['ts_code']
                code = ts_code.split('.')[0]
                if re.match(r'^\d{6}$', code):
                    logger.info(f"✅ 包含匹配: {company_name} -> {row['name']} ({code})")
                    return code
        
        # 3. 反向包含匹配（股票名包含在公司名中）
        match = self.stock_basic_df[self.stock_basic_df['name'].apply(lambda x: x in company_name)]
        if not match.empty:
            for _, row in match.iterrows():
                ts_code = row['ts_code']
                code = ts_code.split('.')[0]
                if re.match(r'^\d{6}$', code):
                    logger.info(f"✅ 反向匹配: {company_name} -> {row['name']} ({code})")
                    return code
        
        logger.warning(f"⚠️ 未找到公司: {company_name}")
        return None
    
    def find_company_name(self, stock_code: str) -> str:
        """
        根据股票代码查找公司名称
        
        Args:
            stock_code: 股票代码（6位数字或带后缀）
            
        Returns:
            公司名称，找不到返回None
        """
        if not stock_code or self.stock_basic_df.empty:
            return None
        
        # 清理代码
        stock_code = stock_code.strip()
        if '.' in stock_code:
            stock_code = stock_code.split('.')[0]
        
        if not re.match(r'^\d{6}$', stock_code):
            return None
        
        # 查找匹配
        match = self.stock_basic_df[self.stock_basic_df['ts_code'].str.startswith(stock_code, na=False)]
        if not match.empty:
            name = match.iloc[0]['name']
            logger.info(f"✅ 代码查找: {stock_code} -> {name}")
            return name
        
        return None
    
    def validate_and_correct_companies(self, companies: list) -> list:
        """
        验证和修正公司列表中的股票代码
        
        Args:
            companies: 公司列表，每个元素是{'name': ..., 'code': ...}
            
        Returns:
            验证后的公司列表
        """
        validated = []
        
        for company in companies:
            name = company.get('name', '')
            code = company.get('code', '')
            
            # 如果已经有有效代码，验证一下
            if code and re.match(r'^\d{6}$', code):
                # 检查代码是否真实存在
                real_name = self.find_company_name(code)
                if real_name:
                    # 更新公司名为真实名称
                    validated.append({
                        'name': real_name,
                        'code': code
                    })
                    logger.info(f"✅ 代码验证成功: {real_name} ({code})")
                    continue
            
            # 没有有效代码，尝试通过公司名查找
            if name:
                found_code = self.find_stock_code(name)
                if found_code:
                    validated.append({
                        'name': self.find_company_name(found_code) or name,
                        'code': found_code
                    })
                else:
                    logger.warning(f"⚠️ 无法匹配公司: {name}")
        
        # 限制数量
        max_companies = 5
        if len(validated) > max_companies:
            logger.info(f"📊 限制分析公司数量: {len(validated)} -> {max_companies}")
            validated = validated[:max_companies]
        
        return validated


# 单例实例
_validator_instance = None


def get_validator() -> CompanyCodeValidator:
    """获取验证器单例"""
    global _validator_instance
    if _validator_instance is None:
        _validator_instance = CompanyCodeValidator()
    return _validator_instance


def find_stock_code(company_name: str) -> str:
    """便捷方法：根据公司名称查找股票代码"""
    return get_validator().find_stock_code(company_name)


def find_company_name(stock_code: str) -> str:
    """便捷方法：根据股票代码查找公司名称"""
    return get_validator().find_company_name(stock_code)


def validate_and_correct_companies(companies: list) -> list:
    """便捷方法：验证和修正公司列表"""
    return get_validator().validate_and_correct_companies(companies)
