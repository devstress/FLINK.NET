#!/usr/bin/env python3
"""
MCP Server for Flink.NET Documentation
Provides access to project documentation and wiki content
"""

import os
import json
import asyncio
from pathlib import Path
from typing import Dict, List, Any

class FlinkDotNetDocsServer:
    def __init__(self):
        self.project_root = Path(os.getenv("PROJECT_ROOT", "."))
        self.docs_path = Path(os.getenv("DOCS_PATH", "./docs/wiki"))
        self.cache = {}
    
    async def initialize(self):
        """Initialize the documentation server"""
        print("Initializing Flink.NET Documentation MCP Server")
        await self.load_documentation()
    
    async def load_documentation(self):
        """Load and cache documentation files"""
        if not self.docs_path.exists():
            print(f"Documentation path not found: {self.docs_path}")
            return
        
        for md_file in self.docs_path.glob("*.md"):
            try:
                content = md_file.read_text(encoding='utf-8')
                self.cache[md_file.stem] = {
                    "path": str(md_file),
                    "content": content,
                    "title": self.extract_title(content)
                }
            except Exception as e:
                print(f"Error loading {md_file}: {e}")
    
    def extract_title(self, content: str) -> str:
        """Extract title from markdown content"""
        lines = content.split('\n')
        for line in lines:
            if line.startswith('# '):
                return line[2:].strip()
        return "Untitled"
    
    async def get_project_context(self) -> Dict[str, Any]:
        """Get comprehensive project context"""
        context = {
            "project_name": "Flink.NET",
            "description": "A .NET-native stream processing framework inspired by Apache Flink",
            "current_status": "Alpha/foundational development stage",
            "key_components": [
                "JobManager - Job coordination and management",
                "TaskManager - Task execution and local state management", 
                "Checkpointing - Barrier-based fault tolerance",
                "Stream Processing API - DataStream operations and operators"
            ],
            "technology_stack": [
                ".NET 8.0",
                "gRPC for inter-service communication",
                "Protobuf for message serialization", 
                ".NET Aspire for local development",
                "Redis and Kafka for external systems"
            ],
            "documentation_available": list(self.cache.keys())
        }
        return context
    
    async def get_documentation(self, topic: str) -> Dict[str, Any]:
        """Get documentation for a specific topic"""
        if topic in self.cache:
            return self.cache[topic]
        
        # Try to find partial matches
        matches = [key for key in self.cache.keys() if topic.lower() in key.lower()]
        if matches:
            return {
                "matches": matches,
                "suggestion": f"Did you mean one of: {', '.join(matches)}?"
            }
        
        return {"error": f"Documentation for '{topic}' not found"}
    
    async def search_documentation(self, query: str) -> List[Dict[str, Any]]:
        """Search documentation content"""
        results = []
        query_lower = query.lower()
        
        for key, doc in self.cache.items():
            if query_lower in doc["content"].lower() or query_lower in doc["title"].lower():
                # Extract relevant snippet
                content_lines = doc["content"].split('\n')
                relevant_lines = []
                for i, line in enumerate(content_lines):
                    if query_lower in line.lower():
                        start = max(0, i - 2)
                        end = min(len(content_lines), i + 3)
                        relevant_lines.extend(content_lines[start:end])
                        break
                
                results.append({
                    "document": key,
                    "title": doc["title"],
                    "snippet": '\n'.join(relevant_lines[:10]),  # Limit snippet size
                    "path": doc["path"]
                })
        
        return results

    async def handle_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP requests"""
        if method == "get_project_context":
            return await self.get_project_context()
        elif method == "get_documentation":
            topic = params.get("topic", "")
            return await self.get_documentation(topic)
        elif method == "search_documentation":
            query = params.get("query", "")
            return await self.search_documentation(query)
        else:
            return {"error": f"Unknown method: {method}"}

async def main():
    """Main entry point for the MCP server"""
    server = FlinkDotNetDocsServer()
    await server.initialize()
    
    print("Flink.NET Documentation MCP Server ready")
    print(f"Loaded {len(server.cache)} documentation files")
    
    # Keep server running
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())