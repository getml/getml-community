# getML docker runtime
## Multiplatform builds
> [!IMPORTANT]
> Multiplatform runtime (and wheel) builds require the build artifacts/packages to present in `$GETML_BUILD_OUTPUT_DIR` (default `build`) for all supported architectures (amd64, arm64). However, currently, multiplatform builds are only supported for the go-related parts of getML (app/cli). To build real multiplatform docker runtimes (and wheels), you need to place a getml package built for the non-native architecture alongside the native one inside `$GETML_BUILD_OUTPUT_DIR`. 
>
> Before running a multiplatform build, the artifacts folder should have roughly the following shape:
> ```
> build/
> └── getml-community-<version-amd64-linux/
>     ├── bin/
>     │  └── engine*
>     ├── lib/
>     │  ├── libgomp.so.1.0.0*
>     │  ├── libgomp.so.1*
>     │  └── libmariadb.so.3*
>     ├── getML*
>     ├── config.json
>     ├── environment.json
>     ├── jwks.pub.json
>     ├── INSTALL.md
>     ├── shape-main.png
>     └── LICENSE.txt
>     getml-community-<version>-arm64-linux/
>     ├── bin/
>     │  └── engine*
>     ├── lib/
>     │  ├── libgomp.so.1.0.0*
>     │  ├── libgomp.so.1*
>     │  └── libmariadb.so.3*
>     ├── getML*
>     ├── config.json
>     ├── environment.json
>     ├── jwks.pub.json
>     ├── INSTALL.md
>     ├── shape-main.png
>     └── LICENSE.txt
> ```
> If you want to build for a single platform, you can override the target's `platform` setting: `./scripts/getml build runtime -p=<linux/arm64 linux/amd64>`

As the docker runtime is multiplatform. You need [a docker engine with multiplafom build support enabled](https://docs.docker.com/build/building/multi-platform/).

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
../scripts/getml build runtime
```

## Running image
```bash
docker run -it getml/getml -p 1708-1733:11708-11733
```

## Publishing (pushing) the image
```bash
../scripts/getml publish runtime
```
