/*
The fbhttplogger package contains a http.Handler wrapper that sends logs requests to a Forest Bus topic.

This is intended as an example of how Forest Bus can be integrated into a standard Go http handler application.
The NewHandler function takes a http.Handler and a forestbus.MessageBatcher and generates json messages to the MessageBatcher for each HTTP request sent to the handler.
*/
package fbhttplogger

import (
	"encoding/json"
	"github.com/owlfish/forestbus"
	"log"
	"net/http"
)

/*
RequestLog defines the JSON format used to log requests and HTTP response information to a Forest Bus topic.
*/
type RequestLog struct {
	actualResponseWriter http.ResponseWriter
	RequestHeaders       http.Header
	Method               string
	Url                  string
	Host                 string
	RemoteAddr           string
	Referer              string
	UserAgent            string
	ResponseHeaders      http.Header
	ResponseCode         int
	ResponseBodySize     int
}

func (log *RequestLog) Header() http.Header {
	return log.actualResponseWriter.Header()
}

func (log *RequestLog) Write(data []byte) (int, error) {
	n, err := log.actualResponseWriter.Write(data)
	log.ResponseBodySize += n
	return n, err
}

func (log *RequestLog) WriteHeader(code int) {
	log.actualResponseWriter.WriteHeader(code)
	log.ResponseCode = code
}

/*
An http.Handler that provides Forest Bus logging.  Create using the NewHandler function.
*/
type Handler struct {
	msgBatcher     *forestbus.MessageBatcher
	wrappedHandler http.Handler
	blockIfFull    bool
}

/*
Creates a http.Handler object that logs all requests to Forest Bus by creating and sending a RequestLog object in JSON format.

blockIfFull blocks the goroutine handling the request if the Message Batcher is full.
*/
func NewHandler(wrappedHandler http.Handler, batcher *forestbus.MessageBatcher, blockIfFull bool) *Handler {
	handler := &Handler{wrappedHandler: wrappedHandler, blockIfFull: blockIfFull, msgBatcher: batcher}
	return handler
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	logMsg := &RequestLog{actualResponseWriter: writer}
	logMsg.RequestHeaders = request.Header
	logMsg.Method = request.Method
	logMsg.Url = request.URL.String()
	logMsg.Host = request.Host
	logMsg.RemoteAddr = request.RemoteAddr
	logMsg.Referer = request.Referer()
	logMsg.UserAgent = request.UserAgent()
	handler.wrappedHandler.ServeHTTP(logMsg, request)
	// Get the resulting values
	if logMsg.ResponseCode == 0 {
		logMsg.ResponseCode = http.StatusOK
	}
	logMsg.ResponseHeaders = writer.Header()
	// Fire off the event to Forest Bus
	jsonMsg, err := json.Marshal(logMsg)
	if err == nil {
		sendErr := handler.msgBatcher.AsyncSendMessage(jsonMsg, nil, nil, handler.blockIfFull)
		if sendErr != nil {
			log.Printf("Warning: Error from AsyncSendMessage: %v\n", sendErr)
		}
	} else {
		log.Printf("Warning: Unable to marshal request information to JSON: %v\n", err)
	}
}
