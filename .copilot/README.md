# Flink.NET AI Assistant Configuration

This directory contains custom instructions, configurations, and tools to enhance AI-assisted development for the Flink.NET project.

## Files Overview

### Core Configuration
- **`instructions.md`**: Custom instructions for GitHub Copilot with project-specific guidance
- **`ai-context.md`**: Comprehensive project context documentation for AI agents
- **`mcp-config.json`**: Model Context Protocol configuration for GitHub Copilot

## Quick Start

### For GitHub Copilot Users
GitHub Copilot will automatically use the instructions in `instructions.md` when working in this repository.

### For GitHub MCP Configuration
Add the MCP configuration from `mcp-config.json` to your GitHub repository or organization settings:

1. Go to your GitHub repository settings
2. Navigate to "Copilot" → "Model Context Protocol"
3. Add the configuration from `mcp-config.json`

This will enable GitHub Copilot to access project documentation and context directly through GitHub's native MCP support.

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
- Direct access to wiki content through GitHub Copilot
- API documentation
- Getting started guides
- Advanced topics and tutorials

## Usage Examples

### Getting Project Context
Ask GitHub Copilot questions like:
- "What's the current architecture of Flink.NET?"
- "How do I implement a new operator?"
- "What are the testing patterns used in this project?"
- "How does checkpoint coordination work?"

### Code Generation
GitHub Copilot can help with:
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
3. **Update MCP Config**: Modify `mcp-config.json` to add new resources or capabilities
4. **Document Patterns**: Update documentation when new development patterns emerge

The goal is to make GitHub Copilot as effective as possible at understanding and contributing to Flink.NET development.