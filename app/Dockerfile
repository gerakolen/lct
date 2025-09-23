FROM python:3.12-slim AS builder

ARG APP_USER="nobody"
ARG APP_GROUP="nobody"

USER $APP_USER:$APP_GROUP
WORKDIR $HOME
COPY requirements.txt ./
RUN python3 -m pip install --user --no-cache-dir -r "$HOME/requirements.txt"

FROM python:3.12-slim

ARG APP_USER="nobody"
ARG APP_GROUP="nobody"

USER $APP_USER:$APP_GROUP
WORKDIR $HOME

COPY --from=builder --chown=$APP_USER:$APP_GROUP "$HOME/.local" "$HOME/.local"
COPY --chown=$APP_USER:$APP_GROUP app/ app

ENV PATH="${PATH}:${HOME}/.local/bin"

EXPOSE 8998
