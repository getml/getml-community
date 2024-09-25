# getML - Automated Feature Engineering for Relational Data and Time Series

getML ([https://getml.com](https://getml.com)) is a tool for automating feature engineering on relational data and time series. Unlike similar tools, it includes a database Engine 
that has been customized for this very purpose.

Because of this customized database Engine, it is very fast. In fact, it is
between 60x to 1000x faster than other open-source tools for automated
feature engineering.

This is the official Python API for the getML Engine.

For more information, visit the [getML documentation](https://getml.com) or explore the [Python API documentation](https://getml.com/latest/reference).

### Build

To build the package install `hatch`
```bash
pip install hatch
```

and run
```bash
hatch build -t wheel
```
