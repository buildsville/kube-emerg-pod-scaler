FROM golang:alpine AS builder

RUN apk update && \
    apk add git build-base && \
    rm -rf /var/cache/apk/* && \
    mkdir -p "$GOPATH/src/github.com/buildsville/" && \
    git clone https://github.com/buildsville/kube-emerg-pod-scaler.git && \
    mv kube-emerg-pod-scaler "$GOPATH/src/github.com/buildsville/" && \
    cd "$GOPATH/src/github.com/buildsville/kube-emerg-pod-scaler" && \
    GOOS=linux GOARCH=amd64 go build -o /kube-emerg-pod-scaler

FROM alpine:3.7

RUN apk add --update ca-certificates

COPY --from=builder /kube-emerg-pod-scaler /kube-emerg-pod-scaler

ENTRYPOINT ["/kube-emerg-pod-scaler","-logtostderr"]
