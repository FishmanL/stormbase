import inspect

import yaml

import opendp.whitenoise.core as wn  # pylint: disable=import-error

with open("config.yaml") as a_yaml_file:
    parsed_yaml_file = yaml.load(a_yaml_file, Loader=yaml.FullLoader)
    debug_pw = parsed_yaml_file["debug_pw"]
    debug_mode = bool(parsed_yaml_file["debug_mode"])


def get_class_from_frame(fr):
    args, _, _, value_dict = inspect.getargvalues(fr)
    # we check the first parameter for the frame function is
    # named 'self'
    if len(args) and args[0] == 'self':
        # in that case, 'self' will be referenced in value_dict
        instance = value_dict.get('self', None)
        if instance:
            # return its class
            return getattr(instance, '__class__', None)
    # return None otherwise
    return None


class CoreWrapper(object):
    def __init__(self, dataset, priv_budget=10):
        self._analysis = wn.Analysis()
        self._analysis.__enter__()
        if isinstance(dataset, dict):
            self._dataset = wn.Dataset(value=dataset, column_names=dataset.keys())
        elif isinstance(dataset, list):
            self._dataset = wn.Dataset(value=dataset, num_columns=1)

        else:
            raise ValueError("more complex types not yet handled")
        self._filterresult = wn.Dataset(value=[], num_columns=1)
        self._analysis.exit()
        self.priv_budget = priv_budget

        self.priv_used = 0

    def _internalexec(self, priv_usage, method, data, *args, **kwargs):
        self._analysis.__enter__()
        tempval = method(data, *args, **kwargs, privacy_usage={'epsilon': priv_usage})
        self._analysis.exit()
        self._analysis.release()
        val = tempval.value
        usage = wn.parse_privacy_usage(tempval.actual_privacy_usage.values[0])
        self.priv_used += usage['epsilon']
        return val

    def mean(self, priv_usage, data, *args, **kwargs):
        self._analysis.__enter__()
        data = wn.to_float(data)
        self._analysis.exit()
        if abs(self.priv_used - self.priv_budget) < priv_usage:
            priv_usage = self.priv_budget - self.priv_used
        return self._internalexec(priv_usage, wn.dp_mean, data, *args, **kwargs)

    def internal_mean(self, priv_usage, *args, **kwargs):
        return self.mean(priv_usage, self._dataset, *args, **kwargs)

    def filter(self, mask, **kwargs):
        self._filterresult = wn.filter(self._dataset, mask, **kwargs)

    def count(self, data, priv_usage, *args, **kwargs):
        if abs(self.priv_used - self.priv_budget) < priv_usage:
            priv_usage = self.priv_budget - self.priv_used
        return self._internalexec(priv_usage, wn.dp_count, data, *args, **kwargs)

    # NOTE:  only here for demo, will be removed in prod
    def reset(self, admin_pw):
        if admin_pw != debug_pw or not debug_mode:
            return "Debug mode and admin access required."
        else:
            self.priv_used = 0
            return "successful reset"

    def __getattribute__(self, name):
        if name == '_dataset' or name == "_filterresult":
            frame = inspect.stack()[1][0]
            tryclass = get_class_from_frame(frame)
            if not isinstance(tryclass, type) or not issubclass(tryclass, CoreWrapper):
                del frame
                raise Exception("Protected attribute!")
            del frame
            return object.__getattribute__(self, name)

        else:
            # Default behaviour
            return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name == 'priv_used':
            frame = inspect.stack()[1][0]
            tryclass = get_class_from_frame(frame)
            if not isinstance(tryclass, type) or not issubclass(tryclass, CoreWrapper):
                del frame
                raise Exception("Protected attribute!")
            del frame
            return object.__setattr__(self, name, value)

        else:
            # Default behaviour
            return object.__setattr__(self, name, value)


nwrap = CoreWrapper([10, 20, 30, 40])


if __name__ == '__main__':
    print(nwrap.internal_mean(data_lower=0.0, data_upper=40.0, data_n=4, priv_usage=0.65))
    print(nwrap.priv_used)
    print(nwrap.internal_mean(data_lower=0.0, data_upper=40.0, data_n=4, priv_usage=40))
    print(nwrap.priv_used)
