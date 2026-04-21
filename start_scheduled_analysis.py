"""
启动定时分析任务
"""
import argparse
from tasks.scheduled_analyzer import ScheduledAnalyzer


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description='启动定时分析任务')
    parser.add_argument('--industry', type=str, help='要分析的行业')
    parser.add_argument('--time', type=str, default='10:00', help='执行时间，格式如 "10:00"')
    parser.add_argument('--multiple', action='store_true', help='启用多行业分析模式')
    parser.add_argument('--immediate', action='store_true', help='立即执行分析任务，不设置定时')
    
    args = parser.parse_args()
    
    analyzer = ScheduledAnalyzer()
    
    if args.immediate:
        # 立即执行分析任务
        if args.industry:
            print(f"立即分析行业: {args.industry}")
            analyzer.analyze_industry(args.industry)
        elif args.multiple:
            print("立即分析多个行业")
            industries = [
                '白酒',
                '半导体',
                '医药',
                '银行',
                '房地产'
            ]
            for industry in industries:
                analyzer.analyze_industry(industry)
        else:
            print("请指定要分析的行业，或使用 --multiple 选项分析多个行业")
        return
    
    if args.multiple:
        # 多行业分析模式
        industries = [
            ('白酒', '10:00'),
            ('半导体', '11:00'),
            ('医药', '14:00'),
            ('银行', '15:00'),
            ('房地产', '16:00')
        ]
        
        for industry, time_str in industries:
            analyzer.schedule_industry_analysis(industry, time_str)
    elif args.industry:
        # 单行业分析模式
        analyzer.schedule_industry_analysis(args.industry, args.time)
    else:
        # 交互式输入行业
        industry = input("请输入要分析的行业: ")
        time_str = input("请输入执行时间（格式如 10:00）: ")
        if not time_str:
            time_str = '10:00'
        analyzer.schedule_industry_analysis(industry, time_str)
    
    # 启动定时任务
    analyzer.start()
    
    print("定时分析任务已启动，按 Ctrl+C 停止")
    
    try:
        while True:
            pass
    except KeyboardInterrupt:
        analyzer.stop()
        print("定时分析任务已停止")


if __name__ == "__main__":
    main()
