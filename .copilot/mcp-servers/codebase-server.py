#!/usr/bin/env python3
"""
MCP Server for Flink.NET Codebase Analysis
Provides insights into project structure, patterns, and implementation details
"""

import os
import json
import asyncio
from pathlib import Path
from typing import Dict, List, Any
import re

class FlinkDotNetCodebaseServer:
    def __init__(self):
        self.project_root = Path(os.getenv("PROJECT_ROOT", "."))
        self.solution_paths = os.getenv("SOLUTION_PATHS", "").split(";")
        self.patterns_cache = {}
        self.structure_cache = {}
    
    async def initialize(self):
        """Initialize the codebase analysis server"""
        print("Initializing Flink.NET Codebase Analysis MCP Server")
        await self.analyze_project_structure()
        await self.identify_patterns()
    
    async def analyze_project_structure(self):
        """Analyze the project structure"""
        structure = {
            "solutions": [],
            "projects": {},
            "key_directories": {}
        }
        
        # Analyze solutions
        for sol_path in self.solution_paths:
            if sol_path.strip():
                full_path = self.project_root / sol_path.strip()
                if full_path.exists():
                    structure["solutions"].append({
                        "path": str(full_path),
                        "name": full_path.stem
                    })
        
        # Find all .csproj files
        for csproj in self.project_root.rglob("*.csproj"):
            project_name = csproj.stem
            structure["projects"][project_name] = {
                "path": str(csproj),
                "directory": str(csproj.parent),
                "type": self.classify_project_type(project_name),
                "references": await self.extract_project_references(csproj)
            }
        
        # Identify key directories
        key_dirs = ["FlinkDotNet", "FlinkDotNetAspire", "FlinkDotNet.WebUI", "docs", "scripts"]
        for dir_name in key_dirs:
            dir_path = self.project_root / dir_name
            if dir_path.exists():
                structure["key_directories"][dir_name] = {
                    "path": str(dir_path),
                    "contents": [item.name for item in dir_path.iterdir() if item.is_dir()]
                }
        
        self.structure_cache = structure
    
    def classify_project_type(self, project_name: str) -> str:
        """Classify project type based on name patterns"""
        if ".Tests" in project_name:
            return "test"
        elif "JobManager" in project_name:
            return "job_manager"
        elif "TaskManager" in project_name:
            return "task_manager"
        elif "Core" in project_name:
            return "core"
        elif "Connectors" in project_name:
            return "connector"
        elif "WebUI" in project_name:
            return "ui"
        elif "Aspire" in project_name:
            return "aspire"
        else:
            return "library"
    
    async def extract_project_references(self, csproj_path: Path) -> List[str]:
        """Extract project references from csproj file"""
        try:
            content = csproj_path.read_text(encoding='utf-8')
            # Simple regex to find ProjectReference includes
            pattern = r'<ProjectReference\s+Include="([^"]+)"'
            matches = re.findall(pattern, content)
            return matches
        except Exception as e:
            print(f"Error reading {csproj_path}: {e}")
            return []
    
    async def identify_patterns(self):
        """Identify common patterns in the codebase"""
        patterns = {
            "grpc_services": [],
            "operators": [],
            "state_management": [],
            "testing_patterns": [],
            "configuration_patterns": []
        }
        
        # Find gRPC service patterns
        for cs_file in self.project_root.rglob("*.cs"):
            try:
                content = cs_file.read_text(encoding='utf-8')
                
                # Look for gRPC service implementations
                if "ServiceBase" in content and "override" in content:
                    patterns["grpc_services"].append({
                        "file": str(cs_file),
                        "class": self.extract_class_name(content)
                    })
                
                # Look for operator implementations
                if any(interface in content for interface in ["IMapOperator", "IFilterOperator", "ISinkFunction", "ISourceFunction"]):
                    patterns["operators"].append({
                        "file": str(cs_file),
                        "class": self.extract_class_name(content),
                        "interfaces": self.extract_implemented_interfaces(content)
                    })
                
                # Look for state management
                if any(state_type in content for state_type in ["IValueState", "IListState", "IMapState"]):
                    patterns["state_management"].append({
                        "file": str(cs_file),
                        "class": self.extract_class_name(content)
                    })
                
                # Look for test patterns
                if "[Test]" in content or "[Fact]" in content:
                    patterns["testing_patterns"].append({
                        "file": str(cs_file),
                        "framework": "xUnit" if "[Fact]" in content else "NUnit"
                    })
                    
            except Exception as e:
                continue  # Skip files that can't be read
        
        self.patterns_cache = patterns
    
    def extract_class_name(self, content: str) -> str:
        """Extract primary class name from C# content"""
        # Simple regex to find class declarations
        pattern = r'public\s+class\s+(\w+)'
        match = re.search(pattern, content)
        return match.group(1) if match else "Unknown"
    
    def extract_implemented_interfaces(self, content: str) -> List[str]:
        """Extract implemented interfaces from C# content"""
        # Look for interface implementations
        patterns = [
            r':\s*([I]\w+)',
            r'implements?\s+([I]\w+)'
        ]
        interfaces = []
        for pattern in patterns:
            matches = re.findall(pattern, content)
            interfaces.extend(matches)
        return list(set(interfaces))  # Remove duplicates
    
    async def get_development_patterns(self) -> Dict[str, Any]:
        """Get common development patterns"""
        return {
            "patterns_found": self.patterns_cache,
            "recommendations": {
                "grpc_services": "Inherit from generated ServiceBase classes and implement async methods",
                "operators": "Implement operator interfaces in Core.Abstractions and add tests",
                "state_management": "Use StateDescriptor for registration and proper serialization",
                "testing": "Follow xUnit patterns with proper mocking of dependencies"
            },
            "architecture_notes": [
                "Services communicate via gRPC with Protobuf messages",
                "State is managed through checkpoint barriers",
                "Dependency injection is used throughout",
                "Async/await patterns for I/O operations"
            ]
        }
    
    async def analyze_similar_code(self, functionality: str) -> Dict[str, Any]:
        """Analyze existing code for similar functionality"""
        results = {
            "similar_implementations": [],
            "patterns_to_follow": [],
            "suggested_approach": ""
        }
        
        functionality_lower = functionality.lower()
        
        # Search through patterns for similar functionality
        for pattern_type, items in self.patterns_cache.items():
            if any(keyword in functionality_lower for keyword in ["operator", "transform", "process"]) and pattern_type == "operators":
                results["similar_implementations"].extend(items)
                results["patterns_to_follow"].append("Follow operator interface implementations")
            
            elif any(keyword in functionality_lower for keyword in ["service", "grpc", "api"]) and pattern_type == "grpc_services":
                results["similar_implementations"].extend(items)
                results["patterns_to_follow"].append("Inherit from ServiceBase and implement async methods")
            
            elif any(keyword in functionality_lower for keyword in ["state", "checkpoint", "storage"]) and pattern_type == "state_management":
                results["similar_implementations"].extend(items)
                results["patterns_to_follow"].append("Use StateDescriptor pattern for state registration")
        
        return results
    
    async def handle_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP requests"""
        if method == "get_project_structure":
            return self.structure_cache
        elif method == "get_development_patterns":
            return await self.get_development_patterns()
        elif method == "analyze_similar_code":
            functionality = params.get("functionality", "")
            return await self.analyze_similar_code(functionality)
        else:
            return {"error": f"Unknown method: {method}"}

async def main():
    """Main entry point for the MCP server"""
    server = FlinkDotNetCodebaseServer()
    await server.initialize()
    
    print("Flink.NET Codebase Analysis MCP Server ready")
    print(f"Analyzed {len(server.structure_cache.get('projects', {}))} projects")
    print(f"Found {sum(len(items) for items in server.patterns_cache.values())} code patterns")
    
    # Keep server running
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())