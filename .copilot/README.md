# Flink.NET AI Assistant Configuration

This directory contains custom instructions, configurations, and tools to enhance AI-assisted development for the Flink.NET project.

## Files Overview

### Core Configuration
- **`instructions.md`**: Custom instructions for GitHub Copilot with project-specific guidance
- **`ai-context.md`**: Comprehensive project context documentation for AI agents
- **`mcp-config.json`**: Model Context Protocol server configuration

### MCP Servers
- **`mcp-servers/docs-server.py`**: Documentation and wiki content server
- **`mcp-servers/codebase-server.py`**: Project structure and pattern analysis server  
- **`mcp-servers/testing-server.py`**: Testing guidance and pattern server

### Scripts
- **`start-mcp-servers.sh`**: Linux/macOS script to start MCP servers
- **`Start-MCP-Servers.ps1`**: Windows PowerShell script to start MCP servers

## Quick Start

### For GitHub Copilot Users
GitHub Copilot will automatically use the instructions in `instructions.md` when working in this repository.

### For AI Agents with MCP Support
1. Start the MCP servers:
   ```bash
   # Linux/macOS
   ./.copilot/start-mcp-servers.sh
   
   # Windows  
   .\.copilot\Start-MCP-Servers.ps1
   ```

2. Configure your AI tool to use the MCP configuration in `mcp-config.json`

### For Manual Reference
- Review `ai-context.md` for comprehensive project understanding
- Check `../AGENTS.md` for development environment setup
- Reference project documentation in `../docs/wiki/`

## What This Provides

### Project Understanding
- Complete architecture overview
- Technology stack and dependencies
- Current implementation status
- Development patterns and conventions

### Development Assistance  
- Code pattern recognition
- Testing strategy guidance
- Build and deployment procedures
- Troubleshooting common issues

### Documentation Access
- Searchable wiki content
- API documentation
- Getting started guides
- Advanced topics and tutorials

## Usage Examples

### Getting Project Context
Ask AI agents questions like:
- "What's the current architecture of Flink.NET?"
- "How do I implement a new operator?"
- "What are the testing patterns used in this project?"
- "How does checkpoint coordination work?"

### Code Generation
AI agents can help with:
- Implementing new gRPC services following project patterns
- Creating operators with proper state management
- Writing unit and integration tests
- Adding new connectors with exactly-once semantics

### Troubleshooting
Get help with:
- Build and compilation issues
- Integration test setup problems
- gRPC communication debugging
- Performance optimization guidance

## Contributing to AI Assistance

To improve AI assistance for the project:

1. **Update Instructions**: Modify `instructions.md` with new patterns or guidelines
2. **Enhance Context**: Add information to `ai-context.md` for better project understanding
3. **Extend MCP Servers**: Add new functionality to the Python MCP servers
4. **Document Patterns**: Update documentation when new development patterns emerge

The goal is to make AI agents as effective as possible at understanding and contributing to Flink.NET development.