"""
API server module.
"""
from flask import Flask, jsonify as flask_jsonify
from flask_cors import CORS
from ..config.settings import API_PREFIX
from .json_fix import jsonify

def create_app():
    """
    Create and configure the Flask application.
    """
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes
    
    # Configure JSON response to not escape non-ASCII characters
    app.config['JSON_AS_ASCII'] = False
    app.json.ensure_ascii = False
    
    # Register API blueprints
    from .movies import movies_bp
    from .screenings import screenings_bp
    
    # Register blueprints with URL prefix
    app.register_blueprint(movies_bp, url_prefix=f'{API_PREFIX}/movies')
    app.register_blueprint(screenings_bp, url_prefix=f'{API_PREFIX}/screenings')
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(500)
    def server_error(error):
        return jsonify({"error": "Internal server error"}), 500
    
    # Health check endpoint
    @app.route(f'{API_PREFIX}/health', methods=['GET'])
    def health_check():
        return jsonify({"status": "ok"})
    
    # Chinese test endpoint
    @app.route(f'{API_PREFIX}/chinese-test', methods=['GET'])
    def chinese_test():
        return jsonify({
            "message": "中文测试成功",
            "data": {
                "电影": "柏林苍穹下",
                "描述": "柏林由两位天使守护着，一个是对人世疾苦冷眼旁观的卡西尔，另一个是常常感怀于人类疾苦的丹密尔。",
                "导演": "文德斯",
                "年份": "1987"
            }
        })
    
    return app

def run_server(app, host='0.0.0.0', port=5000, debug=False):
    """
    Run the Flask server.
    """
    app.run(host=host, port=port, debug=debug) 