module HttpSup.JWT where

import Control.Lens ((?~), (^.), view, set)
import Control.Monad.Except
import Control.Monad.IO.Class (liftIO)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Function ((&))
import Data.Text (Text)
import qualified Data.Text as DT

import Crypto.JWT
import qualified Crypto.JWT as Jose

import Data.Aeson (eitherDecodeFileStrict, encodeFile)

import qualified Servant.Auth.Server as Sas


generateKeyPair :: MonadRandom m => m JWK
generateKeyPair = do
  k <- genJWK . ECGenParam $ P_256
  pure $ k
    & jwkAlg ?~ JWSAlg ES256
    & jwkKeyOps ?~ [Sign, Verify]
    & jwkUse ?~ Sig


tmpJWK :: IO JWK
tmpJWK = do
  jwk <- genJWK (RSAGenParam (4096 `div` 8))
  -- let
  --  h = view thumbprint jwk :: Digest SHA256
  --  kid = view (re (base64url . digest) . utf8) h
  pure $ set jwkKid Nothing jwk


-- | Save a new jwk and its public version to a file.
saveNewJWKToFile :: FilePath -> IO (Either String JWK)
saveNewJWKToFile path = do
  keyPair <- generateKeyPair
  let mbPubJWK = keyPair ^. asPublicKey
  case mbPubJWK of
    Nothing -> pure $ Left "~@[saveNewJWKToFile] public JWK generation error."
    Just pubJWK -> do
      encodeFile (path <> ".pub") pubJWK
      encodeFile path keyPair
      pure $ Right keyPair


-- | Load the JWK from a file (in standard JSON format).
readJWK :: FilePath -> IO JWK
readJWK path = do
  eJWK <- eitherDecodeFileStrict path
  case eJWK of
    Left e -> fail e
    Right keyPair -> pure keyPair


verifyJWT' :: Sas.FromJWT a => Sas.JWTSettings -> BS.ByteString -> IO (Either Text a)
verifyJWT' jwtCfg input = do
  verifiedJWT <- liftIO $
    runExceptT . withExceptT formJWTError $ do
      unverifiedJWT <- Jose.decodeCompact (BSL.fromStrict input)
      valKeys <- liftIO $ Sas.validationKeys jwtCfg
      let
        validationSettings = jwtSettingsToJwtValidationSettings jwtCfg
      Jose.verifyClaims validationSettings valKeys unverifiedJWT
  pure $ verifiedJWT >>= Sas.decodeJWT
  where
    formJWTError :: JWTError -> Text
    formJWTError = DT.pack . show


jwtSettingsToJwtValidationSettings :: Sas.JWTSettings -> Jose.JWTValidationSettings
jwtSettingsToJwtValidationSettings s =
  defaultJWTValidationSettings (toBool <$> Sas.audienceMatches s)
  where
    toBool Sas.Matches = True
    toBool Sas.DoesNotMatch = False
