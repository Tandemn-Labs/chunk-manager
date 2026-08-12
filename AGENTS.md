# AGENTS.md

This file is for coding agents. It is laid out as organization wide rules followed by repo-specific information.

Current repo: tandemn-labs/chunk-manager

## Organization guide

### Overall coding style
- Avoid clever one-liners that hurt readability.
- Use comments only for non-obvious operational logic, failure modes, or cross-service contracts. Do not comment what the code already says.
- Follow the existing local patterns before inventing a new one.
- Simplicity first. No features beyond what was asked. No abstractions for single-use code. No "flexibility" or "configurability" that wasn't requested. No error handling for impossible scenarios. Do not add unnecessary complexity in order to attain goals like scalability and security.
- Make only surgical changes. Touch only what is needed, don't improve or refractor anything that is not absolutely necessary.
- Work backwards; Define the GOAL first (success criteria) then ASK QUESTIONS till verified. Your goal is to transform the goal into sub-tasks and verifiable goals. For multi-step taks, state a brief plan.

### Testing Philosophy
- Integration tests should use local containers; never real cloud accounts.

### Repository Boundaries
- Do not commit credentials, .env files, generated caches, local Docker volumes, or large artifacts.

## Repo-specific guide

This repository is for the chunk manager service to support batched inference tasks. This service is part of a larger, comprehensive inference framework. Tandemn's placement algorithm takes in inference jobs and current resources across all environments (cloud + on-prem), and then plans out the placement of jobs. Furthermore, the algorithm will track metrics and changing job + resource maps, and change the placement of jobs in an adaptive manner to support more efficient use of limited resources. 

This chunk manager runs in a central control plane and is used to manage distribution of chunks across all workers progressing on the same job.
