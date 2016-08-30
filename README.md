# ZipG

ZipG is a distributed graph store that serves queries on compressed graphs.

## Building ZipG and running benchmarks

**NOTE:** This section assumes you are building ZipG on the following Amazon EC2 AMIs (based on your region)

```
"ap-northeast-1/hvm": "ami-bfbfd4be"
"ap-northeast-1/pvm": "ami-c7bfd4c6"
"ap-southeast-1/hvm": "ami-de96c08c"
"ap-southeast-1/pvm": "ami-c096c092"
"ap-southeast-2/hvm": "ami-8565fbbf"
"ap-southeast-2/pvm": "ami-ff65fbc5"
"eu-west-1/hvm": "ami-2ae0165d"
"eu-west-1/pvm": "ami-1ae0166d"
"sa-east-1/hvm": "ami-692f8f74"
"sa-east-1/pvm": "ami-612f8f7c"
"us-east-1/hvm": "ami-35b1885c"
"us-east-1/pvm": "ami-5bb18832"
"us-west-1/hvm": "ami-72320f37"
"us-west-1/pvm": "ami-7a320f3f"
"us-west-2/hvm": "ami-ae6e0d9e"
"us-west-2/pvm": "ami-9a6e0daa"
```

The steps required to build ZipG on other AMIs may vary slightly.

### Single Machine Setup

The instance type should be r3.8xlarge. The following instructions assume that zipg is cloned at at `$HOME`, and `$ZIPG_HOME` is set to `$HOME/zipg`

1. Clone the ZipG repository:

```bash
git clone https://github.com/anuragkh/zipg.git
```

Use the following credentials: 

username: vldbreviewer
password: vldbreviewer

2. Install dependencies

```bash
bash $ZIPG_HOME/ec2/install-dependencies.sh
```

3. Run cmake

```bash
cd $ZIPG_HOME
cmake .
```

4. Compile ZipG sources

```
cd $ZIPG_HOME
make -j
```

5. Install ZipG Java client library

```
cd $ZIPG_HOME/client/java
mvn clean install
```

Note that this requires Thrift 1.0.0 java libraries to be installed in your `$CLASSPATH`.

6. Install linkbench

```
cd $ZIPG_HOME/linkbench
mvn clean install -DskipTests
```

Now you should be ready to run the benchmarks. You may need to edit configuration files at:

* `$ZIPG_HOME/conf/succinct_env.sh`
* `$ZIPG_HOME/conf/hosts`
* `$ZIPG_HOME/linkbench/config/LinkBenchConfigZipG.properties`
* `$ZIPG_HOME/linkbench/scripts/zipg_master`
* `$ZIPG_HOME/linkbench/scripts/zipg_clients`
* `$ZIPG_HOME/linkbench/scripts/zipg_servers`

to ensure that the configuration parameters are consistent with your setup. The configuration files should contain comments to guide you in setting parameter values.

Also, make sure you have passwordless ssh access to localhost.

Finally, you can use scripts at `$ZIPG_HOME/linkbench/scripts` to run benchmarks. In particular, the following command should run the benchmark, provided the appropriate configuration parameters have been set:

```
$ZIPG_HOME/linkbench/scripts/bench_zipg.sh <dataset-name>
```

where `<dataset-name>` is the name of the dataset. The datasets used in the paper are available at `s3://succinct-datasets/linkbench/succinct`.

### Distributed cluster

Use 10x m3.2xlarge `servers` for the distributed cluster; additionally a `master` instance for monitoring and controlling benchmark processes on different nodes, and 10x `client` instances for running benchmark client processes are also required. To run the benchmarks:

1. Follow the build instructions under `Single Machine Setup` to install zipg on all `servers` and the `master`

2. Follow the build instructions under `Single Machine Setup` to install linkbench benchmark tools on all `clients` and `master`

3. At the `master`, update the following files as described:

`$ZIPG_HOME/linkbench/scripts/zipg_master` : Set this to localhost
`$ZIPG_HOME/linkbench/scripts/zipg_clients` : Set this to the list of client ips/hostnames.
`$ZIPG_HOME/linkbench/scripts/zipg_servers` : Set this to the list of server ips/hostnames.

4. Ensure that all instances have passwordless ssh access to each other, and ports `11001-11099` are open on the `server` instances.

5. Finally set the configuration parameters on the `master` as described in the `Single Machine Setup`.

Now you can use the same scripts at `$ZIPG_HOME/linkbench/scripts` to run benchmarks on the distributed benchmarks, e.g.,

```bash
$ZIPG_HOME/linkbench/scripts/bench_zipg.sh <dataset-name>
```

where `<dataset-name>` is the name of the dataset. The datasets used in the paper are available at `s3://succinct-datasets/linkbench/succinct`. To download the required dataset, use the script `$ZIPG_HOME/scripts/download_data.sh` as follows:

```bash
$ZIPG_HOME/scripts/download_data.sh <dataset-name>
```

## Contact

If you have any doubts or would like to report bugs, please contact us at:

* anuragk [at] berkeley.edu
* ragarwal [at] cornell.edu

Please note that the repo is undergoing rapid changes, and there may be temporary inconsistencies in the instructions and the repo.