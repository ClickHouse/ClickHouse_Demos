"""Read-only Poisson demand inference over the artifact's supported context."""
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from .contracts import WorkshopError

FEATURE_SCHEMA_HASH = hashlib.sha256(b"poisson-price-v1:intercept,sku,store,hour,weekday,log(price/fixed_reference);exposure-offset;utc").hexdigest()


class ModelInvalid(WorkshopError):
    def __init__(self):
        super().__init__("model_invalid", "Load a verified prepared model artifact before creating proposals.")


class ModelUnsupported(WorkshopError):
    def __init__(self):
        super().__init__("model_unsupported", "Choose a subject, trading hour, and price within the model's supported range.")


def feature_names(encoding:dict)->list[str]:
    result=["intercept"]
    for key in ("skus","stores","hours","weekdays"):
        result.extend(f"{key}:{value}" for value in encoding[key][1:])
    return result+["log_price_ratio"]


def design_row(encoding:dict,sku:int,store:int,hour:datetime,price:int,reference:int)->np.ndarray:
    row=[1.]
    for key,value in [("skus",sku),("stores",store),("hours",hour.hour),("weekdays",hour.weekday())]:
        if value not in encoding[key]:
            raise ModelUnsupported()
        row.extend(float(value==level) for level in encoding[key][1:])
    row.append(math.log(price/reference))
    return np.asarray(row,dtype=float)


class DemandModel:
    def __init__(self,artifact:dict):
        try:
            encoding=artifact["encoding"]
            validation=artifact.get('validation',{})
            if (artifact.get('decision_validation')=='failed'
                or any(validation.get(gate) is False for gate in ('demand_passed','decision_passed'))):
                raise ValueError('Rejected validation gates.')
            if artifact["status"]!="ready" or artifact["feature_schema_hash"]!=FEATURE_SCHEMA_HASH:
                raise ValueError("Unverified feature schema or fit.")
            for key in ("skus","stores","hours","weekdays"):
                levels=encoding[key]
                if not levels or levels!=sorted(set(levels)):
                    raise ValueError("Invalid feature vocabulary.")
            arrays=[np.asarray(artifact["coefficients"],dtype=float)]+[np.asarray(c,dtype=float) for c in artifact["ensemble"]]
            for coef in arrays:
                if coef.shape!=(len(feature_names(encoding)),) or not np.isfinite(coef).all() or coef[-1]>0:
                    raise ValueError("Invalid coefficients.")
            if not artifact["ensemble"] or not artifact["subjects"]:
                raise ValueError("Missing support/ensemble.")
            for sku,store in artifact["subjects"]:
                if sku not in encoding["skus"] or store not in encoding["stores"] or artifact["references"][str(sku)]<=0:
                    raise ValueError("Invalid subject/reference.")
                low,high=artifact["supported_price_ratios"][str(sku)]
                if not 0<low<=high or not math.isfinite(high):
                    raise ValueError("Invalid supported price range.")
        except (KeyError,TypeError,ValueError,OverflowError) as error:
            raise ModelInvalid() from error
        self.artifact=artifact
        self.version=artifact["model_version"]
        self.encoding=encoding

    @classmethod
    def from_artifact(cls,path:Path)->"DemandModel":
        try:
            return cls(json.loads(path.read_text()))
        except (OSError,json.JSONDecodeError) as error:
            raise ModelInvalid() from error

    def predict(self,snapshot,price_minor:int,window_start:datetime,exposure_hours:float)->float:
        return self._predict(self.artifact["coefficients"],snapshot,price_minor,window_start,exposure_hours)

    def ensemble_predict(self,snapshot,price_minor:int,window_start:datetime,exposure_hours:float)->list[float]:
        return [self._predict(c,snapshot,price_minor,window_start,exposure_hours) for c in self.artifact["ensemble"]]

    def _predict(self,coef,snapshot,price,start,exposure):
        sku,store=snapshot.subject.sku,snapshot.subject.store_id
        if [sku,store] not in self.artifact["subjects"] or type(price) is not int or price<=0:
            raise ModelUnsupported()
        reference=self.artifact["references"][str(sku)]
        low,high=self.artifact["supported_price_ratios"][str(sku)]
        if snapshot.reference_price_minor!=reference or not low<=price/reference<=high:
            raise ModelUnsupported()
        if not math.isfinite(exposure) or exposure<=0 or exposure>48 or start.tzinfo is None:
            raise ModelInvalid()
        current,end,total=start.astimezone(UTC),start.astimezone(UTC)+timedelta(hours=exposure),0.
        while current<end:
            next_hour=current.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1)
            boundary=min(next_hour,end)
            vector=design_row(self.encoding,sku,store,current,price,reference)
            try:
                rate=math.exp(float(np.dot(vector,coef)))
            except OverflowError as error:
                raise ModelInvalid() from error
            total+=rate*(boundary-current).total_seconds()/3600
            current=boundary
        if not math.isfinite(total) or total<=0:
            raise ModelInvalid()
        return total
