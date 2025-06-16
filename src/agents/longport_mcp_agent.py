import asyncio
import os
import logging
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from mcp_use import MCPClient, MCPAgent

# # 配置全局日志，打印 DEBUG 及以上级别日志
# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

class LongportMcpAgentWrapper:
    def __init__(self):
        load_dotenv()
        config = {
            "mcpServers": {
                "longport": {
                    "type": "longport",
                    "command": r"C:\Users\29397\.mcp\longport\longport-mcp.exe",
                    "args": ["--log-dir", "./mcp_logs"],
                    "env": {
                        "LONGPORT_APP_KEY": os.getenv("LONGPORT_APP_KEY"),
                        "LONGPORT_APP_SECRET": os.getenv("LONGPORT_APP_SECRET"),
                        "LONGPORT_ACCESS_TOKEN": os.getenv("LONGPORT_ACCESS_TOKEN"),
                    }
                }
            }
        }

        print(">>> 初始化 MCPClient ...")
        self.client = MCPClient.from_dict(config)

        print(">>> 初始化 DeepSeek LLM ...")
        llm = ChatOpenAI(
            # siliconflow
            # api_key=os.getenv("SiliconFlow_API_KEY"),
            # base_url="https://api.siliconflow.cn/v1",
            # model="deepseek-ai/DeepSeek-R1"
            
            # deepseek
            api_key=os.getenv("DEEPSEEK_API_KEY"),
            base_url="https://api.deepseek.com/v1",
            model="deepseek-chat"
        )

        print(">>> 创建 MCPAgent，开启 verbose 模式 ...")
        self.agent = MCPAgent(llm=llm, client=self.client, max_steps=30, verbose=True)

    async def longport_mcp_ask(self, user_text):
        try:
            return await self.agent.run(user_text)
        except Exception as e:
            print("❌ 执行失败：", str(e))

# if __name__ == "__main__":
#     asyncio.run(longport_mcp_agent())