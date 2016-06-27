# Alooma's Python SDK
Manage your real-time data pipeline with Alooma's Python SDK. Our Python SDK allows you to perform all the operations enabled by our UI and more with Python code.

## Getting started
1. Import and instantiate the API:

```Python
import alooma
api = alooma.Alooma(hostname="<your sub-domain>.alooma.io", username="<your username>", password="<your password>")
```

2. Use it! The API has submodules corresponding to Alooma's features.
 Here are some examples:
 - Get your currently deployed code from the Code Engine:
   ``` Python
   code = api.code_engine.get_code()
   ```
 - Deploy new code into the code engine:
   ``` Python
   api.code_engine.deploy_code(new_code)
   ```
   Where `new_code` is a str representation of valid Python code.

 - Get all the event-types the system knows:
   ``` Python
   types = api.mapper.get_event_types()
   ```
 - Start a Restream:
   ``` Python
   api.restream.start_restream()
   ```
 - And many many more!
