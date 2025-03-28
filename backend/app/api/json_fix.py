"""
自定义jsonify函数，确保中文字符正确显示而不是Unicode转义序列
"""
import json
from flask import Response, current_app

def jsonify(*args, **kwargs):
    """确保中文字符正确显示的jsonify函数"""
    if args and kwargs:
        raise TypeError('jsonify() behavior undefined when passed both args and kwargs')
    
    data = args[0] if len(args) == 1 else args or kwargs
    
    return Response(
        json.dumps(data, ensure_ascii=False),
        mimetype='application/json; charset=utf-8'
    ) 