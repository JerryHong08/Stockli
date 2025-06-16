import requests

url = 'https://api.yesapikey.com/v1/chat/completions'
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer sk-J6bma5oaGyLzWKGx7b60D85712F3476dB493Bb8aB743787f'  # 替换为你自己的key
}

# 对话历史初始化
messages = [{"role": "system", "content": "You are a helpful assistant."}]

print("开始聊天，输入 'exit' 退出对话。")

while True:
    user_input = input("你：")
    if user_input.lower() in ["exit", "quit", "退出"]:
        print("结束对话。")
        break

    # 添加用户输入到消息历史中
    messages.append({"role": "user", "content": user_input})

    # 构造请求数据
    data = {
        "model": "gpt-4o-2024-05-13",
        "messages": messages,
        # "temperature": 1,
        # "max_tokens": 200,
        # "top_p": 1
    }

    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print("助手：", content)

            # 添加 assistant 的回复到消息历史
            messages.append({"role": "assistant", "content": content})
        else:
            print("请求失败：", response.status_code, response.text)
    except Exception as e:
        print("请求异常：", e)
