# getML docker runtime
## Multiplatform builds
> [!IMPORTANT]
> Until we have native cross compilation for the engine, it is too costly to  build for the foreign platform through emulation. Therefore, we have introduced `...copy-artifacts` variations for `python` and `docker` targets which effectively just look for [build artifacts](https://www.notion.so/5cfaed1347af454d83261e4ea772b303?pvs=21) inside `GETML_BUILD_OUTPUT_DIR` (default `build`) and copy them over. So you have to ensure that an up-to-date version of all build artifacts is present inside `build` . Kindly ask someone on the team to provide artifacts for the foreign platform.
> 
> If everything is present, `build`  should have roughly the following structure:
> 
> ```bash
> build/
> ├── getml-community-<version>-amd64-linux/
> │   ├── bin/
> │   ├── lib/
> │   └── config.json
> └── getml-community-<version>-arm64-linux/
>     ├── bin/
>     ├── lib/
>     └── config.json
> ```
> 
> You can call `ls -lah build` to check the timestamps of artifacts.
> If you want to build for a single platform, you can override the target's `platform` setting: `./scripts/getml build runtime -p=<linux/arm64 linux/amd64>`

As the docker runtime is multiplatform. You need [a docker engine with multiplatform build support enabled](https://docs.docker.com/build/building/multi-platform/).

The recommended way is [to use the `containerd` image store](https://docs.docker.com/build/building/multi-platform/#enable-the-containerd-image-store), which supports multiplatform images out of the box.

To enable the `containerd` image store, add the following to your `/etc/docker/daemon.json`:
```json
{
  "features": {
    "containerd-snapshotter": true
  }
}
```


After changing the configuration, restart the docker daemon:
```bash
sudo systemctl restart docker
```

If you are using the Docker Engine (not Docker Desktop), also make sure that you have [setup QEMU](https://docs.docker.com/build/building/multi-platform/#qemu) for emulating the foreign platform.

## Building image
```bash
../scripts/getml build docker-copy-artifacts -p linux/arm64,linux/amd64
```

## Running image
```bash
docker run -it getml/getml -p 1708-1733:11708-11733
```

## Publishing (pushing) the image
```bash
../scripts/getml publish docker
```
