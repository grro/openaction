# OpenAction 

**OpenAction** is an innovative, AI-driven framework for smart home automation. It replaces static, manually created
rules with dynamic scripts generated and managed by an Artificial Intelligence (LLM) based on natural language.

## Core Features

* **Natural Language Automation:** Create complex smart home automations through simple chat dialogues with an LLM (such as Claude).
* **Model Context Protocol (MCP) Native:** * **Sensors & Actuators:** All hardware devices provide their interfaces as independent MCP services.
    * **OpenAction Interface:** The framework itself acts as an MCP server. The AI uses provided tools (like `register_task` or `backup_tasks`) to create and manage automations within the system.
* **Dynamic Scheduling:** An integrated Cron service ensures the precise execution of AI-generated scripts.
* **Persistent State:** Tasks have access to a persistent `Store` to save states across multiple executions.

---

## 🏗 Architecture & Workflow

The system is based on a decentralized architecture connected via the Model Context Protocol (MCP):

1. **The Request:** The user describes an automation in a chat with the AI (e.g., Claude Desktop).
2. **The Translation:** The AI analyzes the prompt, writes a Python script (including a cron rule and an execute function), and uses the `register_task` tool of the **OpenAction MCP Server** to store the script in the system.
3. **The Scheduling:** OpenAction's local `TaskRegistry` and `CronService` monitor the schedules and trigger the tasks at the specified times.
4. **The Execution:** During execution, the script calls the corresponding endpoints of the connected hardware MCP servers (e.g., switching lights, reading temperatures) via the `McpRegistry`.

---

## 🚀 Installation & Startup

### Prerequisites
* Python 3.10 or higher
* A running MCP Client (e.g., Claude Desktop)
* Any sensors/actuators available as MCP servers on your network

### Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/your-username/openaction.git](https://github.com/your-username/openaction.git)
   cd openaction