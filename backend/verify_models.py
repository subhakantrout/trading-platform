import asyncio
import json
import logging
from openai import AsyncOpenAI
import os
import sys

# Try to load config
try:
    import config
    API_KEY = config.NVIDIA_NIM_API_KEY
except ImportError:
    print("Could not import config. Make sure to run this inside the backend folder.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("model_verifier")

if not API_KEY:
    logger.error("NVIDIA_NIM_API_KEY is not set in config. Please set it via the dashboard first.")
    sys.exit(1)

client = AsyncOpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=API_KEY
)

VERIFIED_FILE = "verified_models.json"

async def test_model(model_id: str) -> bool:
    """Test if a model can successfully return a simple JSON payload."""
    try:
        response = await client.chat.completions.create(
            model=model_id,
            messages=[{"role": "user", "content": 'Respond with ONLY valid JSON: {"test": "ok"}'}],
            temperature=0.1,
            max_tokens=10,
            timeout=10.0 # Fast timeout
        )
        content = response.choices[0].message.content.strip()
        # Basic check if it looks like JSON
        if "{" in content and "}" in content:
            return True
        return False
    except Exception as e:
        logger.debug(f"Model {model_id} failed: {e}")
        return False

async def main():
    logger.info("Fetching available models from NVIDIA NIM...")
    try:
        resp = await client.models.list()
        all_models = [m.id for m in resp.data]
    except Exception as e:
        logger.error(f"Failed to fetch models list: {e}")
        return

    logger.info(f"Found {len(all_models)} models. Beginning diagnostic test (this will take 10-20 seconds)...")
    
    # We will test in batches to avoid rate limits
    working_models = []
    
    batch_size = 5
    for i in range(0, len(all_models), batch_size):
        batch = all_models[i:i+batch_size]
        logger.info(f"Testing batch {i//batch_size + 1}: {batch}")
        
        tasks = [test_model(m) for m in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for model_id, result in zip(batch, results):
            if result is True:
                working_models.append(model_id)
                logger.info(f"✅ {model_id} - WORKING")
            else:
                logger.warning(f"❌ {model_id} - FAILED")
                
        await asyncio.sleep(0.5) # small delay between batches
        
    logger.info(f"\nDiagnostic complete. Found {len(working_models)} working models out of {len(all_models)}.")
    
    with open(VERIFIED_FILE, "w") as f:
        json.dump(working_models, f, indent=4)
        
    logger.info(f"Saved working models to {VERIFIED_FILE}. The UI will now only show these models!")

if __name__ == "__main__":
    asyncio.run(main())
