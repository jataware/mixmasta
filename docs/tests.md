# Testing Mixmasta

Tests are available in the `tests` directory. They can be run with:

```
cd tests
python3 -m unittest test_mixmasta.py -v
```

If you have built the Docker container, you can run tests in your container with:

```
docker run -it -w=/tests --entrypoint="python3" jataware/mixmasta:0.5.17 -m unittest test_mixmasta.py -v
```

> Note the `-w` flag changes the working directory within the container to `/tests`.