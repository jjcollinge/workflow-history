# Workflow History 📜
This repository contains a simple example of how to execute a Dapr Workflow and read the workflow execution history using the Dapr SDK for Go. This example ONLY uses the native Dapr APIs and does not rely on anything else.

The program will execute the following workflows:
- Simple
- Chain
- FanOut
- Timer
- SubWorkflow

The computed history includes a hash that can be used to ensure the consistency of the event history. This hash excludes any high cardinality variables and focuses only
on the type and structure of the event history.

The program will error if the workflow history hashes change between executions.

## Prerequisites

- Go 1.18 or later
- Dapr CLI
- Dapr initialized and running

## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/jjcollinge/workflow-history.git
   cd workflow-history
   ```

2. Run the Dapr sidecar:

   ```bash
   dapr run -f ./dapr.yaml
   ```
