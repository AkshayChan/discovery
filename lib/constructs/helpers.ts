let envString: string = 'dev';

if (process.env.DEPLOY_ENV === 'prod') {
  envString = 'prod';
} else if (process.env.DEPLOY_ENV === 'test') {
  envString = 'test';
} else if (process.env.DEPLOY_ENV === 'mobile') {
  envString = 'mobile';
}

// eslint-disable-next-line import/prefer-default-export
const envSpecific = (logicalName: string, underscore?: boolean, slashPrefix?: boolean,
  plainEnv?: boolean) => {
  const suffix = logicalName;

  if (underscore) {
    return `${envString}_${suffix}`;
  }
  if (slashPrefix) {
    return `/${envString}${suffix}`;
  }
  if (plainEnv) {
    return `${envString}`;
  }
  return `${envString}-${suffix}`;
};

// @ts-ignore
// eslint-disable-next-line no-extend-native
const hashCode = function (s) {
  // @ts-ignore
  // eslint-disable-next-line no-param-reassign,no-bitwise
  return s.split('').reduce((a, b) => { a = ((a << 5) - a) + b.charCodeAt(0); return a & a; }, 0);
};

export {
  // eslint-disable-next-line import/prefer-default-export
  envSpecific,
  hashCode,
};
