# OpenAction

**OpenAction** is an AI-native action framework for the Agentic Home.

Move beyond rigid "If-This-Then-That" rules and tedious UI-based configurations. Powered by AI agent technology,
OpenAction replaces traditional smart home solutions with flexible, adaptive logic. It helps to translate natural language
intent into dynamic, executable scripts — giving your smart home the ability to adapt, reason, and act.

For the vision and idea behind OpenAction, please refer to the following article: [From Smart Home to Agentic Home](articles/FromSmartHomeToAgenticHome/FromSmartHomeToAgenticHome.md)

---

## Core Features

*   **Natural Language Automation:** Define complex home behaviors via chat. No more wrestling with nested "If-Then" menus or YAML configurations.
*   **Stateful Intelligence:** An integrated `Store` provides persistence, allowing scripts to save and retrieve data across multiple executions.
*   **Dynamic Scheduling:** A built-in Cron service triggers AI-generated scripts based on time, sensor events, or external API data.
*   **Python Execution:** The AI generates standard Python code, enabling complex calculations, loops, and sophisticated error handling.

---

## Architecture & Workflow

The system acts as a bridge between high-level reasoning (Agents) and low-level hardware control. Here, the AI agent acts as a software
developer who can introspect services and write custom, deterministic Python scripts to control smart home devices.
The workflow is as follows:

1.  **The Intent:** The user describes a goal to an MCP-capable client (e.g., Claude Desktop): *"If it’s dark outside and someone is home, turn on the living room light. If we’re away, simulate presence by randomly toggling lights, ending no later than late in the evening."*
2.  **The Translation:** The Agent uses the **OpenAction MCP Server** tools to inspect available devices and writes a custom Python script.
3.  **The Registration:** OpenAction stores the script and sets up the necessary triggers (e.g., polling a weather API or listening for a sensor change).
4.  **The Execution:** When triggered, the script runs in a local sandbox, calling the endpoints of connected **Sensors & Actuators**.

---
