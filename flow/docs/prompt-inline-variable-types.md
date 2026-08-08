# Inline Variable Types in Prompt Templates

This document describes how to support locking in variable types directly in the prompt text (e.g. `{{user_name: string}}`) so authors can declare types next to placeholders and the UI can avoid or simplify a separate variables form.

## Current behavior

- Templates use plain Handlebars: `{{user_name}}`, `{{current_date}}`.
- Variable **definitions** (name, type, required, default, etc.) live in the version’s `variables` JSON; the template string itself has no type info.
- `core/src/llm/template.rs` has `extract_variable_names()` — it only collects the **name** (stops at the first non-alphanumeric/underscore). So `{{user_name: string}}` would already yield `user_name` and ignore the rest.

The prompt text does not define types today; types only exist in the separate `variables` payload.

## Proposed: inline type in the prompt

Allow optional `: type` after the name so the author can lock in the variable type in the prompt:

- `{{name}}` — unchanged; treat as type `string` (or untyped) for backward compatibility.
- `{{name: type}}` — e.g. `{{user_name: string}}`, `{{count: number}}`, `{{active: boolean}}`, `{{role: enum}}`.

### Why normalize before Handlebars

Handlebars does not understand `{{user_name: string}}` as “variable user_name with type string”. It would try to interpret the whole expression and fail. So at **runtime** we must **normalize** the template before calling Handlebars: replace `{{name: type}}` with `{{name}}`, then compile as today.

### Where to implement

**1. `core/src/llm/template.rs`**

- **Extract variable definitions from the template**  
  Add something like `extract_variable_definitions(template)` that finds all `{{...}}` and parses either `name` or `name: type`, returning e.g. `Vec<(String, String)>` (name, type) or minimal `VariableDefinition`s. Omit type defaults to `string`.
- **Normalize template for Handlebars**  
  Add a function that rewrites `{{name: type}}` → `{{name}}` so the existing `compile_prompt()` keeps working. Use this normalized string when calling Handlebars.

**2. Flow (gateway)**

- **When creating/updating a version**  
  Optionally run `extract_variable_definitions(system_prompt)` and either (a) auto-populate the version’s `variables` array from that, or (b) use it only at apply-time for validation (with optional separate `variables` for extra constraints like required/default/enum).
- **When applying a prompt**  
  Normalize the template (strip `: type`), then validate request variables against the definitions (from inline and/or from stored `variables`), then call existing `compile_prompt`.

**3. UI (Prompt Hub)**

- User types: `You are helping {{user_name: string}}. Today is {{current_date: string}}.`
- **Parse on blur/save**: show a read-only list, e.g. “Variables: user_name (string), current_date (string).” If a variables form is added later, prefill it from the prompt.
- **Or** skip a separate variables form for the simple case and derive name + type entirely from the prompt; advanced fields (required, default, enum values) can live in a separate “variables” section later.

### Backward compatibility

- Keep supporting plain `{{name}}`: treat as type `string` (or leave untyped and skip type validation). Existing prompts continue to work; new ones can add `: string`, `: number`, etc.

### Scope

- **Minimal first step**: inline type only — `{{user_name: string}}` locks in type and is validated at request time; no separate variables form needed for that.
- **Later**: optional separate config for `required`, `default`, `enum` values, `max_chars`, etc., while still using the inline type as the primary type hint.

### JSON extended type definitions (possible direction)

We may adopt **JSON extended type definitions** (e.g. a subset of JSON Schema, JSON Type Definition, or a small custom JSON schema) to describe variable definitions and optionally response shape in a standard, toolable way. Benefits:

- **Variable schema**: Represent `variables` (names, types, required, default, enum values, max_chars, etc.) as a single JSON schema or JTD form. The UI, API, and gateway validation could all consume the same definition; inline `{{name: type}}` could be a shorthand that compiles into or coexists with this schema.
- **Export and CI**: “Export schema” and “contract in CI” (see testing section) could emit and diff this JSON format, enabling codegen, IDE hints, and automated contract tests.
- **Response format**: Flow already supports `response_format` (JSON schema) for structured output. Aligning variable definitions with the same or a related type-definition format would keep one mental model and one set of tooling (validation, codegen) for both inputs and outputs.

If we go this route, inline syntax would remain the lightweight authoring path; the stored or exported canonical form could be the JSON extended type definition.

## Testing app code against the active prompt version

We should make it easy for teams to verify that their application code works with the **active** prompt version (correct `prompt_config` name, required variables, types, and optionally response shape). Ideas:

### CI integration

- **Gateway smoke test in CI**  
  A CI job calls the gateway with the project API key, `prompt_config: "<name>"`, and a minimal `prompt_variables` payload that satisfies the active version’s schema. Assert 200 and optionally that the response has the expected structure (e.g. `choices[0].message.content` non-empty, or JSON schema if using structured output). If the active version’s required variables change, the test fails and blocks merge or deploy until the app is updated.
- **Contract / schema in CI**  
  Provide an API (or CLI) that returns the **active** version’s variable schema (names, types, required) for a given project and config name. The app (or a separate repo) keeps a “contract” file (e.g. JSON or OpenAPI snippet) and CI diffs it against the live schema. Drift fails the build so developers update either the prompt or the app.
- **Version pin in CI (optional)**  
  Allow requests to pass a `prompt_version_id` or `x-reiver-prompt-version` so CI can pin to a specific version. CI runs against that version; if someone promotes a new version, CI doesn’t change until the pin is updated, reducing surprise breakages.

### Other ideas

- **SDK / client helpers**  
  Provide helpers (e.g. in the example client or an official SDK) that take a config name and a map of variables, validate types client-side against a cached or fetched schema, and call the gateway. Reduces “wrong variable name” or “missing required var” errors in development.
- **Schema export**  
  In the Prompt Hub UI or via API, “Export schema” for a config (active version’s variables + types). Teams can commit this and use it in tests or codegen (e.g. generate TypeScript types or a contract test).
- **Playground “Test as API”**  
  In the Playground, a “Copy as curl” or “Copy as test snippet” that includes `prompt_config`, `prompt_variables`, and the project API key (redacted or env var). Developers paste into a test file or CI script for a one-off smoke test.
- **Canary / rollout awareness**  
  When a canary is active, “active” might mean different variants for different requests. CI could either (a) run against the baseline only (e.g. via a header or version pin), or (b) run against the gateway multiple times and assert that all responses are valid, so that both baseline and canary versions are compatible with the app’s variable payload.

### Summary of testing section

The goal is to let teams assert in CI that their code is compatible with the current (or pinned) prompt version: right variables, right types, and optionally response shape. Gateway smoke tests, schema/contract APIs, and optional version pinning are the main levers; SDK helpers and schema export improve DX and make writing those tests easier.

## Summary

Adding inline variable types lets authors lock in types in the prompt version (e.g. `{{user_name: string}}`). Implementation is: (1) parse and normalize in `core/src/llm/template.rs`, and (2) use the extracted definitions in Flow when creating/applying versions and when validating `prompt_variables`.
