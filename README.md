# Traffic Flow Test Scripts

This repository contains the yaml files, docker files, and test scripts to test Traffic Flows in an OVN-Kubernetes k8s cluster.

## Setting up the environment

The package "kubectl" should be installed.

The recommended python version is 3.11 for running the Traffic Flow tests

```
python -m venv tft-venv
source tft-venv/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements.txt
```

### Optional: Developer Environment Setup

If you're planning to contribute or run tests/linters locally, install the developer dependencies to the environment. These include everything from `requirements.txt` (runtime) plus additional tools like `pytest`, `black`, `mypy`, and `flake8`:

```bash
python -m venv tft-venv 
source tft-venv/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements-devel.txt
```

Once installed, you can use:
```bash
pytest         # Run test suite
black .        # Format code
...
```

This step is **optional** and not required for using the Traffic Flow Test scripts.

## Configuration YAML fields:

```
tft:
  - name: "(1)"
    namespace: "(2)"
    # test cases can be specified individually i.e "1,2,POD_TO_HOST_SAME_NODE,6" or as a range i.e. "POD_TO_POD_SAME_NODE-9,15-19"
    test_cases: "(3)"
    duration: "(4)"
    # Location of artifacts from run can be specified: default <working-dir>/ft-logs/
    # logs: "/tmp/ft-logs"
    connections:
      - name: "(5)"
        type: "(6)"
        instances: (7)
        server:
          - name: "(8)"
            persistent: "(9)"
            sriov: "(10)"
            default_network: "(11)"
        client:
          - name: "(12)"
            sriov: "(13)"
            default_network: "(14)"
        plugins:
          - name: (15)
          - name: (15)
        secondary_network_nad: "(16)"
	resource_name: "(17)"
kubeconfig: (18)
kubeconfig_infra: (18)
```

1. "name" - This is the name of the test. Any string value to identify the test.
2. "namespace" - The k8s namespace where the test pods will be run on
3. "test_cases" - A list of the tests that can be run. This can be either a string
     that possibly contains ranges (comma separated, ranged separated by '-'), or a
     YAML list.
    | ID | Test Name            |
    | -- | -------------------- |
    | 1  | POD_TO_POD_SAME_NODE |
    | 2  | POD_TO_POD_DIFF_NODE |
    | 3  | POD_TO_HOST_SAME_NODE |
    | 4  | POD_TO_HOST_DIFF_NODE |
    | 5  | POD_TO_CLUSTER_IP_TO_POD_SAME_NODE |
    | 6  | POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE |
    | 7  | POD_TO_CLUSTER_IP_TO_HOST_SAME_NODE |
    | 8  | POD_TO_CLUSTER_IP_TO_HOST_DIFF_NODE |
    | 9  | POD_TO_NODE_PORT_TO_POD_SAME_NODE |
    | 10 | POD_TO_NODE_PORT_TO_POD_DIFF_NODE |
    | 15 | HOST_TO_POD_SAME_NODE |
    | 16 | HOST_TO_POD_DIFF_NODE |
    | 17 | HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE |
    | 18 | HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE |
    | 19 | HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE |
    | 20 | HOST_TO_CLUSTER_IP_TO_HOST_DIFF_NODE |
    | 21 | HOST_TO_NODE_PORT_TO_POD_SAME_NODE |
    | 22 | HOST_TO_NODE_PORT_TO_POD_DIFF_NODE |
    | 23 | HOST_TO_NODE_PORT_TO_HOST_SAME_NODE |
    | 24 | HOST_TO_NODE_PORT_TO_HOST_DIFF_NODE |
    | 25 | POD_TO_EXTERNAL |
    | 27 | POD_TO_POD_2ND_INTERFACE_SAME_NODE |
    | 28 | POD_TO_POD_2ND_INTERFACE_DIFF_NODE |
    | 29 | POD_TO_POD_MULTI_NETWORK_POLICY |
4. "duration" - The duration that each individual test will run for.
5. "name" - This is the connection name. Any string value to identify the connection.
6. "type" - Supported types of connections are iperf-tcp, iperf-udp, netperf-tcp-stream, netperf-tcp-rr
7. "instances" - The number of instances that would be created. Default is "1"
8. "name" - The node name of the server.
9. "persistent" - Whether to have the server pod persist after the test. Takes in "true/false"
10. "sriov" - Whether SRIOV should be used for the server pod. Takes in "true/false"
11. "default_network" - (Optional) The name of the default network that the sriov pod would use.
12. "name" - The node name of the client.
13. "sriov" - Whether SRIOV should be used for the client pod. Takes in "true/false"
14. "default_network" - (Optional) The name of the default network that the sriov pod would use.
15. "name" - (Optional) list of plugin names
    | Name                    | Description                    |
    | ----------------------- | ------------------------------ |
    | measure_cpu             | Measure CPU Usage              |
    | measure_power           | Measure Power Usage            |
    | validate_offload        | Verify OvS Offload             |
    | validate_bandwidth      | Validate bandwidth thresholds  |
16. "secondary_network_nad" - (Optional) - The name of the secondary network for multi-homing and multi-networkpolicies tests. For tests except 27-29, the primary network will be used if unspecified (the default which is None). For mandatory tests 27-29 it defaults to "tft-secondary" if not set.
17. "resource_name" - (Optional) - The resource name for tests that require resource limit and requests to be set. This field is optional and will default to None if not set, but if secondary network nad is defined, traffic flow test
tool will try to autopopulate resource_name based on the secondary+network_nad provided.
18. "kubeconfig", "kubeconfig_infra": if set to non-empty strings, then these are the KUBECONFIG
  files. "kubeconfig_infra" must be set for DPU cluster mode. If both are empty, the configs
  are detected based on the files we find at /root/kubeconfig.*.

## Running the tests

Simply run the python application as so:

```
./tft.py config.yaml
```

## Environment variables

- `TFT_TEST_IMAGE` specify the test image. Defaults to `ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest`.
     This is mainly for development and manual testing, to inject another container image.
- `TFT_IMAGE_PULL_POLICY` the image pull policy. One of `IfNotPresent`, `Always`, `Never`.
     Defaults to `IfNotPresent`m unless `$TFT_TEST_IMAGE` is set (in which case it defaults
     to `Always`).
- `TFT_PRIVILEGED_POD` sets whether test pods are privileged. This overwrites the settings
     from the configuration YAML.
- `TFT_MANIFESTS_OVERRIDES` to specify an overrides directory for manifests. If not set, the
     default is "manifests/overrides". If set to empty, no overrides are used. You can place
     your own variants of the files from "manifests" directory and they will be preferred.
- `TFT_MANIFESTS_YAMLS` to specify the output directory for rendered manifests. This
     defaults to "manifests/yamls".
- `TFT_KUBECONFIG`, `TFT_KUBECONFIG_INFRA` to overwrite the kubeconfigs from the configuration
     file. See also the "--kubeconfig" and "--kubeconfig-infra" command line options.

## File Transfer via magic-wormhole

It is sometimes cumbersome to transfer files between machines. [magic-wormhole](https://github.com/magic-wormhole/magic-wormhole) helps
with that. Unfortunately it is not packaged in RHEL/Fedora. You can install it with `pip install magic-wormhole` or
```
python3 -m venv /opt/magic-wormhole-venv && \
( source /opt/magic-wormhole-venv/bin/activate && \
  pip install --upgrade pip && \
  pip install magic-wormhole ) && \
ln -s /opt/magic-wormhole-venv/bin/wormhole /usr/bin/
```

wormhole is installed in the kubernetes-traffic-flow-tests container.
From inside the container you can issue `wormhole send $FILE`. Or you can

```
podman run --rm -ti -v /:/host -v .:/pwd:Z -w /pwd ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest wormhole send $FILE
```

This will print a code, which you use on the receiving end via `wormhole receive $CODE`.
Or

```
podman run --rm -ti -v .:/pwd:Z -w /pwd ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest wormhole receive $CODE
```
