import uvicorn
from os import getenv

# from dotenv import load_dotenv

# load_dotenv(".env")

if __name__ == "__main__":
    # si PORT tiene valor se utiliza sino utiliza el puerto 8000
    port = int(getenv("PORT", 9800))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)
