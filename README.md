# Trading Engine

This repository hosts the **Production Engineering** & **Research Engineering** parts of CapitalFund. Follow the steps below to set up your local development environment and start contributing.

---

## 📋 Prerequisites

Before you begin, ensure you have the following:

* **Docker** installed ([Download here](https://docs.docker.com/get-docker/)).
* An **IDE** of your choice (e.g., [PyCharm](https://www.jetbrains.com/pycharm/) or [VS Code](https://code.visualstudio.com/)).
* A **GitHub Account** with access to the organization.
* A valid **`service-account.json`** file (provided by an admin).
* Access granted to the **GitHub Organization**.

---

## 🚀 Setup Instructions

1. **Request Access**
   Contact a repository admin to request access to the GitHub organization and this repository.

2. **Clone the Repository**
   Once you have access, clone the repo:

   ```bash
   git clone https://github.com/<org-name>/trading-engine.git
   cd trading-engine
   ```

   > If you are having trouble cloning the repository, trying using the [GitHub CLI](https://cli.github.com/)

3. **Configure Service Account**
   Place your `service-account.json` in a secure location. Export the path as an environment variable:

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```

## 📚 Additional Resources

* [Docker Documentation](https://docs.docker.com/)
* [GitHub Docs](https://docs.github.com/)
* [Google Cloud Authentication](https://cloud.google.com/docs/authentication/getting-started)

---

Would you like me to expand this into a **full contribution-ready README** (with sections like *Project Overview*, *Contributing*, *Testing*, and *Deployment*), or keep it strictly to **setup and prerequisites** for now?
