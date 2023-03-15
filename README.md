# grupoboticario-case

Case details are located [here](./resources/Case%20-%20Grupo%20Boticário.pdf) and [here](./resources/Case%20Engenharia%20de%20Dados%20-%20Spotify.pdf).

## Installation

Clone the repository with `git`:

```bash
git clone https://github.com/gwyddie/grupoboticario-case
```

Setup up the environment:

```bash
make prep-env
```

Adjust the settings on `.env`. It's also necessary to use a service account to access GCP services, so downlaod the `key_file` to `./service-account.json`, and make it available to Airflow using:

```bash
make import-connection
```

The data to be processed and the resources required by the Dataproc cluster can be uploaded with:

```bash
make ci
```

Lastly, you can start running the containers:

```bash
make up
```

## Tests

I haven't setup overcomplicated unit tests to assert the DAGs' structure, but I included a single one, that I find the most useful, to check if all DAGs can be created without import errors.

```bash
make test
```

## Technologies

- Apache Airflow 2.5.1
- Apache Spark 3.1.3
- Python 3.7
- Cloud Storage
- Dataproc
- BigQuery

---

_That's all, folks!_
