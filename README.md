# OpenAction

**OpenAction** is an AI-native framework for smart home automation that replaces static, If-This-Then-That" rules 
with dynamic scripts. By leveraging the **Model Context Protocol (MCP)**, it allows users to orchestrate their 
home through natural language, turning intent into executable logic.

---

## Core Features

*   **️Natural Language Automation:** Define complex home behaviors via chat. No more wrestling with nested "If-Then" menus or YAML configurations.
*   **Stateful Intelligence:** An integrated `Store` provides persistence, allowing scripts to save and retrieve data across multiple executions.
*   **Dynamic Scheduling:** A built-in Cron service triggers AI-generated scripts based on time, sensor events, or external API data.
*   **Python Execution:** The AI generates standard Python code, enabling complex calculations, loops, and sophisticated error handling.

---

## Architecture & Workflow

The system acts as a bridge between high-level reasoning (LLMs) and low-level hardware control:

1.  **The Intent:** The user describes a goal to an MCP-capable client (e.g., Claude Desktop): *"If it’s dark outside and someone is home, turn on the living room light. If we’re away, activate 'Vacation Mode' to simulate presence by randomly toggling lights for 10 minutes to 4 hours, ending no later than 11 PM."*
2.  **The Translation:** The LLM uses the **OpenAction MCP Server** tools to inspect available devices and writes a custom Python script.
3.  **The Registration:** OpenAction stores the script and sets up the necessary triggers (e.g., polling a weather API or listening for a sensor change).
4.  **The Execution:** When triggered, the script runs in a local sandbox, calling the endpoints of connected **Sensors & Actuators**.

---