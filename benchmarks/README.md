# Benchmarks

## About

The benchmarks evaluate the performance of getML's FastProp algorithm against five other open-source libraries for automated feature engineering on relational data and time series.

Data-Sets:

<details>
  <summary>air_pollution</summary>

  The dataset contains hourly data on air pollution and weather in Beijing, China. The challenge is to predict the pm2.5 concentration for the next hour.

  S. De Vito, E. Massera, M. Piga, L. Martinotto, and G. Di Francia,“On field calibration of an electronic nose for benzene estimation in an urban pollution monitoring scenario,” Sensors and Actuators B: Chemical, vol. 129, no. 2, pp. 750–757, 2008. [Online]. Available: https://www.sciencedirect.com/science/article/pii/S0925400507007691
  
  A detailed demonstration of how to handle this data-set can be found in the [getML-demo repository](https://github.com/getml/getml-demo?tab=readme-ov-file#descriptions)
</details>
<details>
  <summary>dodgers</summary>

  The dataset contains five-minute measurements of traffic near Los Angeles. The traffic volume can be affected by a game hosted by the LA Dodgers in the nearby stadium, but not to the extent that it is very obvious to spot such an event in the data. The LA Dodgers are a popular baseball team from Los Angeles.  The challenge is to predict the traffic volume for the next five-minute interval.

  A. Ihler, J. Hutchins, and P. Smyth, “Adaptive event detection with time-varying poisson processes,” in Proceedings of the 12th ACM SIGKDD international conference on Knowledge discovery and data mining, 2006, pp. 207–216.

  A detailed demonstration of how to handle this data-set can be found in the [getML-demo repository](https://github.com/getml/getml-demo?tab=readme-ov-file#descriptions)
</details>
<details>
  <summary>energy</summary>

  The dataset contains measurements of the electricity consumption of a single household in ten-minute-intervals. The challenge is to predict the energy consumption of all household appliances for the next ten-minute interval.
</details>
<details>
  <summary>interstate94</summary>

  The dataset contains hourly data on traffic volume on the Interstate 94 from Minneapolis to StPaul. The challenge is to predict the traffic volume for the next hour.

  A detailed demonstration of how to handle this data-set can be found in the [getML-demo repository](https://github.com/getml/getml-demo?tab=readme-ov-file#descriptions)
</details>
<details>
  <summary>tetouan</summary>

  The dataset contains the electricity consumption of three different zones in Tetouan City, north Morocco measured in ten-minute intervals. The challenge is to predict the electricity consumption in Zone 1 for the next ten-minute interval.

  A. Salam and A. El Hibaoui, “Comparison of machine learning algorithms for the power consumption prediction:-case study of tetouan city–,” in 2018 6th International Renewable and Sustainable Energy Conference (IRSEC). IEEE, 2018, pp. 1–5.
</details>

Libraries:

* [getML fastprop](https://docs.getml.com/latest/user_guide/feature_engineering/feature_engineering.html#fastprop)
* [tsflex](https://github.com/predict-idlab/tsflex)
* [featuretools](https://www.featuretools.com/)
* [tsfel](https://github.com/fraunhoferportugal/tsfel)
* [tsfresh](https://github.com/blue-yonder/tsfresh)
* [kats](https://github.com/facebookresearch/Kats)

## Build

Build the image with necessary Python version and libraries installed

```bash
$ docker compose build
```

> [!IMPORTANT]  
> Because of the used libraries, this benchmarks only run on the x86_64 architecture.

## Run

Run the benchmarks inside a container based on the build image

```bash
$ docker compose up benchmarks
```

## View logs

The logs written by the benchmarks are written to the terminal in the [Run](#run) step. They can be recalled later via

```bash
$ docker compose logs benchmarks
```

For better reading and scrolling use for example

```bash
$ docker compose logs benchmarks 2>&1 | sed 's/^M/\n/g' | less
```

## Clean

Remove the container, its images, logs and volumes

```bash
$ docker compose down --volumes --rmi all
```

> [!NOTE]  
> *--rmi* for removing images does currently not work with podman compose.   
> Images have to be removed separately for example via `podman image prune --all`