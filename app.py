from flask import Flask, render_template, request, jsonify
import subprocess
from mychat import chat_with_assistant as assistant_chat
from forecast import forecast as fore

app = Flask(__name__)

# 在启动 Flask 服务器之前运行你的 Python 文件
# def run_python_scripts():
#     # 运行第一个 Python 文件
#     subprocess.run(["python", "vis.py"])
#
#     # 运行第二个 Python 文件
#     subprocess.run(["python", "word.py"])
#
# # 在应用程序启动时调用函数
# @app.before_first_request
# def before_first_request():
#     run_python_scripts()

# 路由和视图函数
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process_text', methods=['POST'])
def process_text():
    if request.method == 'POST':
        text = request.form['input_text']  # 假设你的 HTML 中的输入字段名称为 'input_text'
        processed_text = fore(text)
        return jsonify({'result': processed_text})

@app.route('/', methods=['POST'])
def chat_endpoint():
    message = request.json.get('message')

    # 这里调用 chat_with_assistant 函数处理消息，并获取响应
    response_message = assistant_chat(message)

    # 返回响应给客户端
    return jsonify({
        'sender': '新闻小助手',
        'message': response_message
    })
if __name__ == '__main__':
    app.run(debug=True)
