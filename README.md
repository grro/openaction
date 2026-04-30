# OpenAction

**OpenAction** is an AI-driven framework for smart home automation. It replaces static, manually created rules with dynamic scripts generated and managed by an Artificial Intelligence (LLM) based on natural language.

## Core Features

* **Natural Language Automation:** Create complex smart home automations through simple chat dialogues with an LLM (such as Claude).
* **Sensors & Actuators:** All hardware devices provide their interfaces as independent MCP services.
* **OpenAction Interface:** The framework itself acts as an MCP server. The AI uses provided tools to create and manage automations within the system.
* **Dynamic Scheduling:** An integrated Cron service ensures the precise execution of AI-generated scripts.
* **Persistent State:** Tasks have access to a persistent `Store` to save states across multiple executions.

---

## Architecture & Workflow

The system is based on a decentralized architecture connected via the Model Context Protocol (MCP):

1. **The Request:** The user describes an automation in a chat with the AI (e.g., Claude Desktop).
2. **The Translation:** The AI analyzes the prompt, writes a Python script, and uses tools of the **OpenAction MCP Server** to store the script in the system.
3. **The Scheduling:** OpenAction's local services monitor the schedules and trigger the tasks at the specified times and events.
4. **The Execution:** During execution, the script calls the corresponding endpoints of the connected devices such as switching lights, reading temperatures

---
