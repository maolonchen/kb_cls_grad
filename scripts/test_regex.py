# test_regex.py
import re

def match_identifier(text):
    # 首先尝试匹配各种括号形式（英文圆括号、中文圆括号、方括号）
    # 优先匹配带括号的模式，确保括号内有内容
    bracket_pattern = r'(\([^)]+\)|（[^）]+）|\[[^\]]+\])\s*(.*)'
    match = re.search(bracket_pattern, text.strip())
    if match:
        identifier = match.group(1)
        content = match.group(2).strip()
        # 验证括号内不只是空格
        bracket_content = identifier[1:-1].strip()  # 去掉括号，提取中间内容
        if bracket_content:  # 如果括号内有内容
            return identifier, content
    
    # 如果没有匹配到带括号的模式，尝试匹配其他模式
    # 注意：匹配顺序很重要，需要从更具体的模式开始
    patterns = [
        # 匹配 A1-2 这种格式
        r'([A-Z]\d+(?:-\d+)*)\s*(.*)',
        # 匹配 1.2.3 这种格式（多个数字点）
        r'(\d+(?:\.\d+)+[、.]?)\s*(.*)',
        # 匹配 A1. 这种格式
        r'([A-Za-z0-9IVX\u2160-\u216F\u2170-\u217F]+\.+)\s*(.*)',
        # 匹配 A1) 这种格式
        r'([A-Za-z0-9IVX\u2160-\u216F\u2170-\u217F]+\))\s*(.*)',
        # 匹配 A1 这种格式（字母+数字）
        r'([A-Z][A-Z0-9\u2160-\u216F\u2170-\u217F]*)\s*(.*)',
        # 匹配 1. 2. 这种格式
        r'(\d+[、.])\s*(.*)',
        # 匹配 ①②③等编号
        r'([\u2460-\u2473\u2474-\u2487])\s*(.*)',
        # 匹配 一、二、等中文编号
        r'([一二三四五六七八九十]+[、.])\s*(.*)',
        # 匹配最后剩下的数字
        r'(\d+)\s*(.*)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            identifier = match.group(1).rstrip('、.')  # 去除结尾的标点
            content = match.group(2).strip()
            return identifier, content
    
    return None, None

def test_regex():
    """测试正则表达式匹配功能"""
    test_cases = [
        "(Ⅰ)基础地理",
        "(E)实景三维数据产品", 
        "(16)地理场景",
        "(16.2)城市级",
        "[A1]测试内容",
        "A1)测试内容",
        "A1.测试内容",
        "①测试内容",
        "一、测试内容",
        "（一）测试内容",
        "1.测试内容",
        "1.2.3测试内容",
        "A1-2测试内容",
        "A1 测试内容",
        "A1-1 测试内容",
        "（二）其他内容",
        "（III）罗马数字",
        "[IV]罗马数字",
        "②第二个",
        "十、十字符号",
        "（十）十字符号",
        "5.2.1 测试内容",
        "12.3.4.5 测试内容",
        "(12.3.4.5) 测试内容",
        "(Ⅱ)罗马数字2",
        "(Ⅲ)罗马数字3",
        "(Ⅳ)罗马数字4",
        "(Ⅴ)罗马数字5",
        "(Ⅵ)罗马数字6",
        "(Ⅶ)罗马数字7",
        "(Ⅷ)罗马数字8",
        "(Ⅸ)罗马数字9",
        "(Ⅹ)罗马数字10",
        "()空括号",  # 这个不应该匹配，因为括号内为空
        "( )空括号2",  # 这个也不应该匹配，因为括号内只有空格
        "A1-1 测试内容",
        "B2.3 测试内容",
        "（）空括号测试",  # 这个也不应该匹配
        "(ABC123)字母数字混合",
        "（包含中文字符）测试",
        "[包含方括号]测试内容",
        "A123 测试内容",
        "B23 测试内容",
        "C1 测试内容"
    ]
    
    print("测试正则表达式匹配：")
    for test_case in test_cases:
        code, text_part = match_identifier(test_case)
        if code:
            print(f"  输入: {test_case}")
            print(f"  匹配成功 - 标识符: '{code}', 文本: '{text_part}'")
        else:
            print(f"  输入: {test_case}")
            print(f"  未匹配")
        print()

if __name__ == "__main__":
    test_regex()