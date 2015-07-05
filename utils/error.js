var ErrNone = 100;
var ErrTopicNotExisted = 101;
var ErrLineNotExisted = 102;
var ErrNotDelivered = 103;
var ErrBadKey = 104;
var ErrTopicExisted = 105;
var ErrLineExisted = 106;
var ErrBadRequest = 400;
var ErrInternalError = 500;

var errorMap = {
    // 404
    ErrNone: "No Message",
    ErrTopicNotExisted: "Topic Not Existed",
    ErrLineNotExisted: "Line Not Existed",
    ErrNotDelivered: "Message Not Delivered",

    // 400
    ErrBadKey: "Bad Key Format",
    ErrTopicExisted: "Topic Has Existed",
    ErrLineExisted: "Line Has Existed",
    ErrBadRequest: "Bad Client Request",

    // 500
    ErrInternalError: "Internal Error",
}

var errorStatus = {
    ErrNone: 404, // http.StatusNotFound,
    ErrTopicNotExisted: 404, // http.StatusNotFound,
    ErrLineNotExisted: 404, // http.StatusNotFound,
    ErrNotDelivered: 404, // http.StatusNotFound,
    ErrInternalError: 500 // http.StatusInternalServerError,
}

function UQError(message, cause) {
    this.name = 'UQError';
    this.message = message;
    this.cause = cause;
    Error.captureStackTrace(this, UQError);
}
UQError.prototype = new Error;

// func NewError(errorCode int, cause string) * Error {
//     return &Error {
//         ErrorCode: errorCode,
//         Message: errorMap[errorCode],
//         Cause: cause,
//     }
// }

// // Only for error interface
// func(e Error) Error() string {
//     return ItoaQuick(e.ErrorCode) + " " + e.Message + " (" + e.Cause + ")"
// }

// func(e Error) statusCode() int {
//     status, ok: = errorStatus[e.ErrorCode]
//     if !ok {
//         status = http.StatusBadRequest
//     }
//     return status
// }

// func(e Error) toJsonString() string {
//     b, _: = json.Marshal(e)
//     return string(b)
// }

// func(e Error) WriteTo(w http.ResponseWriter) {
//     w.Header().Set("Content-Type", "application/json")
//     w.WriteHeader(e.statusCode())
//     fmt.Fprintln(w, e.toJsonString())
// }
