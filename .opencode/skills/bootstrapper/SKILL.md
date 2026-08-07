---
name: bootstrapper
description: Analyzes repo state and optimizes OpenCode artifacts for free, secure, local use. Updates .opencodeignore, opencode.json, agent definitions, and AGENTS.md based on current project state.
license: MIT
---

# Sovereign Bootstrapper Skill

When invoked, perform the following sequence to keep this repo's OpenCode configuration optimal.

## Verified Schema (opencode.json)

Valid top-level keys (verified against opencode binary):
```json
{
  "$schema": "https://opencode.ai/config.json",
  "model": "provider/model-name",
  "provider": {
    "ollama": { "api": "http://localhost:11434/v1" }
  },
  "agent": {
    "agent-name": {
      "name": "...",
      "description": "...",
      "model": "provider/model",
      "mode": "primary|subagent",
      "instruction": "...",
      "tools": { "bash": true, "read": true, "write": true, "edit": true, "glob": true, "grep": true, "webfetch": false, "task": true, "todowrite": false, "list": true, "codesearch": true },
      "permission": { "bash": "allow|ask|deny", "read": "allow", "write": "ask", "edit": "allow", "doom_loop": "ask" }
    }
  }
}
```

**INVALID keys** (do not use): `api_base`, `root`, `baseURL`, `url`, `endpoint`

## Agent File Format (.opencode/agent/NAME.md)

Agents can also be defined as markdown files with YAML frontmatter:
```markdown
---
name: agent-name
description: what it does
model: ollama/deepseek-coder-v2
mode: primary
tools:
  bash: true
  read: true
---
System instruction goes here as markdown body.
```

## Workflow

1. **Check available Ollama models**: `ollama list` — use `deepseek-coder-v2` for coding tasks, `llama3.1` for lighter/general tasks

2. **Update .opencodeignore**: Scan for:
   - New generated directories (build artifacts, caches)
   - New secret-adjacent files
   - New large binary assets
   Compare against current `.opencodeignore` and add missing entries

3. **Validate opencode.json**: Run `opencode debug config` — fix any validation errors immediately

4. **Review agent definitions** in `.opencode/agent/`:
   - Check that instructions reflect current architecture (new endpoints, new services, version bumps)
   - Update `atlantis-dev.md` with any new subsystems or patterns added since last bootstrap
   - Ensure `security-auditor.md` covers new attack surfaces

5. **Update AGENTS.md**: Reflect current agent roster and usage guide

6. **Safety check**: Verify no agent has `webfetch: true` (no outbound HTTP from local model) and no agent can read `*.env*` without `ask` permission

## Trigger Conditions

Re-run bootstrapper after:
- Any new subsystem added (new router, new service, new `viva_api/` package)
- Version bump (update version references in agent instructions)
- New external dependency added (check security-auditor coverage)
- New deploy target or namespace added (update deploy agent)
- After merging a large PR (update architecture map in atlantis-dev)
