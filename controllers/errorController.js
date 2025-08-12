import trackAppStatus from "../models/trackAppStatusModel.js";
import CustomError from "./../helpers/CustomError.js";

const httpStatusCodes = {
  OK: 200,
  BAD_REQUEST: 400,
  NOT_FOUND: 404,
  INTERNAL_SERVER: 500,
  PARTIAL_CONTENT: 206,
};

const devErrors = async (res, error) => {
  await trackAppStatus.create({
    last_modified: new Date().toLocaleString(),
    status: error.statusCode || httpStatusCodes.INTERNAL_SERVER,
    success: false,
    message: error.message || "Something went wrong! Please try again later.",
  });

  res.status(error.statusCode).json({
    status: error.statusCode,
    success: false,
    message: error.message,
    stackTrace: error.stack,
    error: error,
  });
};

const castErrorHandler = (err) => {
  const msg = `Invalid value for ${err.path}: ${err.value}!`;
  return new CustomError(msg, httpStatusCodes.BAD_REQUEST);
};

const duplicateKeyErrorHandler = (err) => {
  // const name = err.keyValue.name;
  const msg = `Duplicate key error`;

  return new CustomError(msg, httpStatusCodes.BAD_REQUEST);
};

const validationErrorHandler = (err) => {
  const errors = Object.values(err.errors).map((val) => val.message);
  const errorMessages = errors.join(". ");
  const msg = `Invalid input data: ${errorMessages}`;

  return new CustomError(msg, httpStatusCodes.BAD_REQUEST);
};

const prodErrors = async (res, error) => {
  await trackAppStatus.create({
    last_modified: new Date().toLocaleString(),
    status: error.statusCode || httpStatusCodes.INTERNAL_SERVER,
    success: false,
    message: error.message || "Something went wrong! Please try again later.",
  });

  if (error.isOperational) {
    res.status(error.statusCode).json({
      status: error.statusCode,
      success: false,
      message: error.message,
    });
  } else {
    res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Something went wrong! Please try again later.",
    });
  }
};

const globalErrorHandler = (error, req, res, next) => {
  // console.log("errorerror", error)
  error.statusCode = error.statusCode || httpStatusCodes.INTERNAL_SERVER;
  error.status = error.status || 500;

  if (process.env.NODE_ENV === "development") {
    devErrors(res, error);
  } else if (process.env.NODE_ENV === "production") {
    if (error.name === "CastError") error = castErrorHandler(error);
    if (error.code === 11000) error = duplicateKeyErrorHandler(error);
    if (error.name === "ValidationError") error = validationErrorHandler(error);
    prodErrors(res, error);
  }
};

//custom try catch function  --->  higher-order function
// const tryCatchFn = (func) => {
//     return (req, res, next) => {
//         func(req, res, next).catch(err => next(err));
//     }
// }

const tryCatchFn = (func) => (req, res, next) => {
  Promise.resolve(func(req, res, next)).catch(next);
};

export { globalErrorHandler, tryCatchFn, httpStatusCodes };
