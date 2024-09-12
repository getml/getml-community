# Usage

The provided notebooks can be checked and used in different ways. 

  * [Reading Online](#reading-online)
  * [Experimenting Locally](#experimenting-locally)
    * [Using Docker](#using-docker)
    * [On the Machine (Linux/x64 & arm64)](#on-the-machine-linuxx64--arm64)

## Reading Online 

As github renders the notebooks, they can each be viewed by just opening and scrolling through them. For convenience, the output of each cells execution is included.

## Experimenting Locally

To experiment with the notebooks, such as playing with different pipelines and predictors, it is best to run them on a local machine. Linux users with an x64 or arm64 architecture can choose from one of the options provided below. Soon, we will offer a simple, container-based solution compatible with all major systems (Windows, Mac) and will also support ARM-based architectures.

### Using Docker

There are a `docker-compose.yml` and a `Dockerfile` for easy usage provided.

Simply clone this repository and command to start the `notebooks` service. The image, it depends on, will be build if it is not already available.

```
$ git clone https://github.com/getml/getml-community.git
$ cd getml-community/demo-notebooks
$ docker compose up notebooks
```

To open Jupyter Lab in the browser, look for the following lines in the output and copy-paste it in your browser:

```
Or copy and paste one of these URLs:

http://localhost:8888/lab?token=<generated_token>
```

> [!NOTE]  
> Using alternatives to [Docker Desktop](https://www.docker.com/products/docker-desktop/) like  
> * [Podman](https://podman.io/),  
> * [Podman Desktop](https://podman-desktop.io/) or  
> * [Rancher Desktop](https://rancherdesktop.io/) with a container engine like dockerd(moby) or containerd(nerdctl)
> 
> allows bind-mounting the notebooks in a user-writeable way (this might need to include `userns_mode: keep-id`) instead of having to `COPY` them in. In combination with volume-binding `/home/getml/.getML/logs` and `/home/getml/.getML/projects`, runs and changes can be persisted across containers.

### On the Machine (Linux/x64 & arm64)

Alternatively, getML and the notebooks can be run natively on the local Linux machine by having certain software installed, like Python and some Python libraries, Jupyter-Lab and the getML Engine. The [getML Python library](https://github.com/getml/getml-community/) provides an Engine version without [Enterprise features](https://www.getml.com/pricing).

The following commands will set up a Python environment with necessary Python libraries and Jupyter-Lab

```
$ git clone https://github.com/getml/getml-community.git
$ cd getml-community/demo-notebooks
$ pipx install hatch
$ hatch env create
$ hatch shell
$ pip install -r requirements.txt
$ jupyter-lab
```

With the last command, Jupyter-Lab should automatically open in the browser. If not, look for the following lines in the output and copy-paste it in your browser:

```
Or copy and paste one of these URLs:

http://localhost:8888/lab?token=<generated_token>
```