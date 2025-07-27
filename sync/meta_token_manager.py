"""
sync/meta_token_manager.py - automatyczne odnawianie Meta Access Token
"""
import os
import logging
import requests
from datetime import datetime, timedelta
import json
import db

logger = logging.getLogger(__name__)

class MetaTokenManager:
    def __init__(self):
        self.app_id = os.environ.get('META_APP_ID')
        self.app_secret = os.environ.get('META_APP_SECRET')
        self.current_token = os.environ.get('META_ACCESS_TOKEN')
        self.api_url = "https://graph.facebook.com/v18.0"

    def validate_token(self, token=None):
        """Sprawdza czy token jest ważny i kiedy wygasa"""
        if not token:
            token = self.current_token
            
        url = f"{self.api_url}/debug_token"
        params = {
            'input_token': token,
            'access_token': token
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json().get('data', {})
                
                if data.get('is_valid'):
                    expires_at = data.get('expires_at')
                    if expires_at:
                        expires_date = datetime.fromtimestamp(expires_at)
                        days_left = (expires_date - datetime.now()).days
                        
                        return {
                            'valid': True,
                            'expires_at': expires_date,
                            'days_left': days_left,
                            'scopes': data.get('scopes', []),
                            'app_id': data.get('app_id')
                        }
                    else:
                        return {
                            'valid': True,
                            'expires_at': 'never'
                        }
                else:
                    return {
                        'valid': False,
                        'error': data.get('error', {}).get('message', 'Token invalid')
                    }
            else:
                return {
                    'valid': False,
                    'error': f"API error: {response.status_code}"
                }
                
        except Exception as e:
            logger.error(f"Error validating token: {str(e)}")
            return {
                'valid': False,
                'error': 'Token validation failed'
            }

    def exchange_for_long_lived_token(self, short_token):
        """Wymienia krótki token na długi (60 dni)"""
        url = f"{self.api_url}/oauth/access_token"
        params = {
            'grant_type': 'fb_exchange_token',
            'client_id': self.app_id,
            'client_secret': self.app_secret,
            'fb_exchange_token': short_token
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                new_token = data.get('access_token')
                expires_in = data.get('expires_in', 0)
                
                logger.info(f"Successfully exchanged token. New token expires in {expires_in} seconds")
                return new_token
            else:
                logger.error(f"Failed to exchange token: HTTP {response.status_code}")
                raise Exception(f"Token exchange failed with status {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error exchanging token: {str(e)}")
            raise Exception("Token exchange request failed")

    def auto_refresh_if_needed(self, days_threshold=30):
        """Automatycznie odświeża token jeśli zostało mniej niż X dni"""
        token_info = self.validate_token()
        
        if not token_info['valid']:
            logger.error(f"Current token is invalid: {token_info.get('error')}")
            return {
                'success': False,
                'error': token_info.get('error'),
                'action': 'manual_refresh_needed'
            }
        
        # Sprawdź czy token ma datę wygaśnięcia
        if token_info.get('expires_at') == 'never':
            logger.info("Token never expires - no refresh needed")
            return {
                'success': True,
                'action': 'no_refresh_needed',
                'reason': 'token_never_expires'
            }
        
        days_left = token_info.get('days_left', 999)
        
        if days_left <= days_threshold:
            logger.info(f"Token expires in {days_left} days (threshold: {days_threshold}). Refreshing...")
            
            try:
                # Wymień aktualny token na nowy długotrwały
                new_token = self.exchange_for_long_lived_token(self.current_token)
                
                # Sprawdź nowy token
                new_token_info = self.validate_token(new_token)
                
                if new_token_info['valid']:
                    # Zapisz nowy token do bazy (opcjonalnie)
                    self.save_new_token(new_token)
                    
                    logger.info("Token refreshed successfully!")
                    
                    return {
                        'success': True,
                        'action': 'token_refreshed',
                        'old_expires_in': days_left,
                        'new_expires_in': new_token_info.get('days_left'),
                        'new_token': new_token,  # W produkcji może warto ukryć
                        'update_required': True
                    }
                else:
                    logger.error(f"New token is invalid: {new_token_info.get('error')}")
                    return {
                        'success': False,
                        'error': 'New token validation failed',
                        'action': 'manual_refresh_needed'
                    }
                    
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                return {
                    'success': False,
                    'error': str(e),
                    'action': 'manual_refresh_needed'
                }
        else:
            logger.info(f"Token is valid for {days_left} more days - no refresh needed")
            return {
                'success': True,
                'action': 'no_refresh_needed',
                'days_left': days_left
            }

    def save_new_token(self, new_token):
        """Zapisuje nowy token do bazy danych"""
        try:
            # Można dodać tabelę dla tokenów w bazie
            logger.info("New token saved to database (placeholder)")
        except Exception as e:
            logger.warning(f"Could not save token to database: {e}")

    def get_token_status(self):
        """Zwraca pełny status tokena dla monitoringu"""
        token_info = self.validate_token()
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'token_valid': token_info['valid'],
            'app_id': self.app_id,
            'token_configured': bool(self.current_token)
        }
        
        if token_info['valid']:
            status.update({
                'expires_at': token_info.get('expires_at').isoformat() if isinstance(token_info.get('expires_at'), datetime) else token_info.get('expires_at'),
                'days_left': token_info.get('days_left'),
                'scopes': token_info.get('scopes', []),
                'requires_refresh': token_info.get('days_left', 999) <= 30
            })
        else:
            status.update({
                'error': token_info.get('error'),
                'requires_manual_intervention': True
            })
        
        return status

# Scheduled job function
def scheduled_token_refresh():
    """Funkcja wywoływana przez scheduler codziennie"""
    logger.info("Starting scheduled token refresh check...")
    
    try:
        manager = MetaTokenManager()
        result = manager.auto_refresh_if_needed(days_threshold=30)
        
        if result['success']:
            if result['action'] == 'token_refreshed':
                logger.info("✅ Token refreshed successfully!")
                # Tu możesz zaktualizować zmienną w Railway
                # Lub wysłać powiadomienie
            else:
                logger.info(f"✅ Token status OK: {result.get('reason', result['action'])}")
        else:
            logger.error(f"❌ Token refresh failed: {result.get('error')}")
            # Wyślij alert
            
        return result
        
    except Exception as e:
        logger.error(f"Scheduled token refresh failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'action': 'scheduler_error'
        }
