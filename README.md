# CRIBA
## A Tool for Comprehensive Analysis of Cryptographic Ransomware's I/O Behavior


CRIBA is an open-source framework that simplifies the exploration, analysis, and comparison of I/O patterns for Linux cryptographic ransomware.

---

## Installation and setup

### CRIBA's tracer and analysis pipeline

CRIBA is built on top of DIO, a generic tool for observing and diagnosing the I/O interactions between applications and in-kernel POSIX storage systems.

CRIBA's modifications to DIO's components are integrated in DIO's repository (release v1.1.0). [https://github.com/dsrhaslab/dio/releases/tag/v1.1.0](https://github.com/dsrhaslab/dio/releases/tag/v1.1.0)

Deployment steps for these components can be found at [https://github.com/dsrhaslab/dio#getting-started-with-dio](https://github.com/dsrhaslab/dio#getting-started-with-dio).

### CRIBA's dashboards

To import CRIBA's dashboard:
- Define the following environment variables:

    - CRIBA_USER - CRIBA's username
    - CRIBA_PASS - CRIBA's password
    - CRIBA_URL  - CRIBA's URL


    <details>
    <summary>Example</summary>

        CRIBA_USER="dio"
        CRIBA_PASS="diopw"
        CRIBA_URL="http://cloud124"

    </details>

- Run the following command to import CRIBA's dashboards:
    ```
    curl -u "$CRIBA_USER:$CRIBA_PASS" -X POST -k "$CRIBA_URL:32222/api/saved_objects/_import" -H "kbn-xsrf: true" --form file=@criba-dashboards.ndjson
    ```

### CRIBA's correlation algorithms

The folder [correlation_algorithms](correlation_algorithms) contains the 6 correlation algorithms provided by CRIBA.

The script [run_ca_darkside](run_ca_darkside) shows how to run each of these scripts for the Darkside family.

[Traces](traces) folder contains the tracers, for each family, obtained with CRIBA's SysTracer and MetricMon components.

---
## Publications
**CRIBA: A Tool for Comprehensive Analysis of Cryptographic Ransomware's I/O Behavior**. Tânia Esteves, Bruno Pereira, Rui Pedro Oliveria, João Marco and João Paulo. _42th International Symposium on Reliable Distributed Systems (SRDS)_, 2023.

---
## Contact

Please contact us at tania.c.araujo@inesctec.pt with any questions.
