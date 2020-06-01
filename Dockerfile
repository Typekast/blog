FROM ruby:2.7

WORKDIR /site

ADD Gemfile /site

RUN bundle install

ADD . /site

CMD bundle exec jekyll serve --port 8080 --host 0.0.0.0
