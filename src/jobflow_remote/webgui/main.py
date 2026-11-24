from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Assuming 'routes.py' is in the same directory or your PYTHONPATH is set up
from jobflow_remote.webgui.routes import router as routes_router

app = FastAPI()

# --- CORRECTED CORS Configuration ---
# Define your specific origins that are allowed
allowed_origins = [
    "http://localhost:5173",    # Your Vite frontend dev server
    "http://127.0.0.1:5173",   # Also common for localhost resolution
    # If you have a deployed frontend, add its URL here too:
    # "https://your.deployed-frontend.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Use your specific list of origins
    allow_credentials=True,         # This is fine with specific origins
    allow_methods=["*"],            # Allows all standard methods
    allow_headers=["*"],            # Allows all headers
)
# --- End CORS Configuration ---

app.include_router(routes_router) # Make sure this router contains all your paths like /actions/...

# The 'origins' variable below this point was not being used by the middleware before.
# It's good practice to define variables before use.

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)