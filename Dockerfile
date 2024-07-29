# Use the dask base image
FROM daskdev/dask

# Set the working directory
WORKDIR /app

# Copy your code into the container
COPY  ./  /app

RUN conda install -c conda-forge pydantic

# expose port
EXPOSE 8786 8787

# The code to run when container is started:
ENTRYPOINT ["python", "/app/wrapper.py"]