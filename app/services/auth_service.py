import time
from datetime import datetime, timedelta, timezone
from app.utils.timezone import now_tz
from typing import Optional
import jwt
from pydantic import BaseModel
from app.core.config import SETTINGS

class TokenData(BaseModel):
    sub: str
    exp: int

class AuthService:
    @staticmethod
    def create_access_token(sub: str, expires_minutes: int | None = None, expires_delta: int | None = None) -> str:
        if expires_delta:
            #If you specify seconds, use seconds
            expire = now_tz() + timedelta(seconds=expires_delta)
        else:
            #Use minutes otherwise
            expire = now_tz() + timedelta(minutes=expires_minutes or SETTINGS.ACCESS_TOKEN_EXPIRE_MINUTES)
        payload = {"sub": sub, "exp": expire}
        token = jwt.encode(payload, SETTINGS.JWT_SECRET, algorithm=SETTINGS.JWT_ALGORITHM)
        return token

    @staticmethod
    def verify_token(token: str) -> Optional[TokenData]:
        import logging
        logger = logging.getLogger(__name__)

        try:
            logger.debug(f"Start checking token.")
            logger.debug(f"Token length:{len(token)}")
            logger.debug(f"JWT key:{SETTINGS.JWT_SECRET[:10]}...")
            logger.debug(f"JWT algorithm:{SETTINGS.JWT_ALGORITHM}")

            payload = jwt.decode(token, SETTINGS.JWT_SECRET, algorithms=[SETTINGS.JWT_ALGORITHM])
            logger.debug(f"Token decoded successfully.")
            logger.debug(f"📋 Payload: {payload}")

            token_data = TokenData(sub=payload.get("sub"), exp=int(payload.get("exp", time.time())))
            logger.debug(f"Token data: sub{token_data.sub}, exp={token_data.exp}")

            #Check for expiry
            current_time = int(time.time())
            if token_data.exp < current_time:
                logger.warning(f"Token has expired: ext.{token_data.exp}, now={current_time}")
                return None

            logger.debug(f"Token was successful.")
            return token_data

        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired.")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Token is invalid:{str(e)}")
            return None
        except Exception as e:
            logger.error(f"Token confirmed the anomaly:{str(e)}")
            return None