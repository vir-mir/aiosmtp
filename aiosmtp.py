import asyncio
import copy
import io
import logging
import re
import smtplib

SMTP_ERROR_CONNECT = -1
SMTP_READY = 220
SMTP_COMPLETED = 250
SMTP_NOT_AVAILABLE = 421
SMTP_START_INPUT = 354

CRLF = '\r\n'


def quoteaddr(addrstring):
    displayname, addr = smtplib.email.utils.parseaddr(addrstring)
    if (displayname, addr) == ('', ''):
        # parseaddr couldn't parse it, use it as is and hope for the best.
        if addrstring.strip().startswith('<'):
            return addrstring
        return "<%s>" % addrstring.strip('"')
    return "<%s>" % addr.strip('"')


def quotedata(data):
    return re.sub(r'(?m)^\.', '..', re.sub(r'(?:\r\n|\n|\r(?!\n))', CRLF, data))


def _quote_periods(bindata):
    return re.sub(br'(?m)^\.', b'..', bindata)


def _fix_eols(data):
    return re.sub(r'(?:\r\n|\n|\r(?!\n))', CRLF, data)


class SMTP(object):
    def __init__(self, hostname='localhost', port=25, loop=None, debug=False, init=True):
        if debug:
            logging.basicConfig(level=3)
        self.hostname = hostname
        self.port = port
        self.loop = loop or asyncio.get_event_loop()
        self.debug = debug
        self.esmtp_extensions = {}
        self.last_helo_status = (None, None)
        self.reader = None
        self.writer = None
        self.ready = asyncio.Future(loop=self.loop)

        if init:
            connected = asyncio.ensure_future(self.connect(), loop=self.loop)
            if not self.loop.is_running():
                self.loop.run_until_complete(connected)

    @asyncio.coroutine
    def connect(self):
        try:
            self.reader, self.writer = yield from asyncio.open_connection(host=self.hostname, port=self.port)
        except (ConnectionRefusedError, OSError) as e:
            message = "Error connection to {} on port {}: {}".format(self.hostname, self.port, e)
            raise smtplib.SMTPConnectError(SMTP_ERROR_CONNECT, message)

        code, message = yield from self.read_line()

        if code != SMTP_READY:
            raise smtplib.SMTPConnectError(code, message)
        if self.debug:
            logging.debug("connected: %s %s", code, message)
        self.ready.set_result(True)

    @asyncio.coroutine
    def reconnect(self):
        self.last_helo_status = (None, None)
        if self.ready:
            self.ready.cancel()
        if self.writer:
            self.writer.close()
        self.ready = asyncio.Future(loop=self.loop)
        yield from self.connect()

    @asyncio.coroutine
    def quit(self):
        code, message = yield from self.execute("quit")
        return code, message

    @asyncio.coroutine
    def close(self):
        self.last_helo_status = (None, None)
        yield from self.quit()
        if self.writer:
            self.writer.close()

    @asyncio.coroutine
    def send_data(self, data):
        if self.debug:
            logging.debug('sending: %s' % data)

        if isinstance(data, str):
            data = bytes(data, 'utf-8')

        self.writer.write(data)

        try:
            yield from self.writer.drain()
        except ConnectionResetError as exc:
            try:
                yield from self.reconnect()
                yield from self.send_data(data)
            except:
                raise exc

    @asyncio.coroutine
    def execute(self, *args):
        command = "{}{}".format(' '.join(args), CRLF)
        if self.debug:
            logging.debug("Sending command: %s", command)
        yield from self.send_data(command)
        res = yield from self.read_line()
        return res

    @asyncio.coroutine
    def helo(self, hostname=None):
        hostname = hostname or self.hostname
        code, message = yield from self.execute("helo", hostname)
        self.last_helo_status = (code, message)
        return code, message

    @asyncio.coroutine
    def ehlo(self, hostname=None):
        hostname = hostname or self.hostname
        code, message = yield from self.execute("ehlo", hostname)

        if code == SMTP_ERROR_CONNECT and len(message) == 0:
            yield from self.close()
            raise smtplib.SMTPServerDisconnected("Server not connected")
        elif code == SMTP_COMPLETED:
            self.parse_esmtp_response(code, message)

        self.last_helo_status = (code, message)
        return code, message

    def parse_esmtp_response(self, code, message):
        response = message.split('\n')
        for line in response[1:]:
            auth_match = re.match(r"auth=(?P<auth>.*)", line, flags=re.I)
            if auth_match:
                auth_type = auth_match.group('auth')[0]
                if 'auth' not in self.esmtp_extensions:
                    self.esmtp_extensions['auth'] = []
                if auth_type not in self.esmtp_extensions['auth']:
                    self.esmtp_extensions['auth'].append(auth_type)
            extensions = re.match(r'(?P<ext>[A-Za-z0-9][A-Za-z0-9\-]*) ?', line)
            if extensions:
                extension = extensions.group('ext').lower()
                params = extensions.string[extensions.end('ext'):].strip()
                if extension == "auth":
                    if 'auth' not in self.esmtp_extensions:
                        self.esmtp_extensions['auth'] = []
                    self.esmtp_extensions['auth'] = params.split()
                else:
                    self.esmtp_extensions[extension] = params
        if self.debug:
            logging.debug("esmtp extensions: %s", self.esmtp_extensions)

    @asyncio.coroutine
    def read_line(self):
        try:
            line = yield from self.reader.read(self.reader._limit)
        except ConnectionResetError as exc:
            raise smtplib.SMTPServerDisconnected(exc)

        try:
            code = int(line[:3])
        except ValueError:
            code = SMTP_ERROR_CONNECT
        message = line[4:].strip(b' \t\r\n').decode()

        if self.debug:
            logging.debug("reply: %s %s", str(code), message)
        if 500 <= code <= 599:
            raise smtplib.SMTPResponseException(code, message)

        return code, message

    def __exit__(self, exc_type, exc_val, exc_tb):
        yield from self.close()

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        yield from self.close()

    @asyncio.coroutine
    def __aenter__(self):
        yield from self.connect()
        return self

    @asyncio.coroutine
    def ehlo_or_helo_if_needed(self):
        if self.last_helo_status == (None, None):
            ehlo_code, ehlo_response = yield from self.ehlo()
            if not (200 <= ehlo_code <= 299):
                helo_code, helo_response = yield from self.helo()
                if not (200 <= helo_code <= 299):
                    raise smtplib.SMTPHeloError(helo_code, helo_response)

    @property
    def supports_esmtp(self):
        return bool(self.esmtp_extensions)

    def supports(self, extension):
        return extension.lower() in self.esmtp_extensions

    @asyncio.coroutine
    def rset(self):
        code, message = yield from self.execute("rset")
        return code, message

    @asyncio.coroutine
    def mail(self, sender, options=None):
        options = options or []
        from_string = "FROM:{}".format(quoteaddr(sender))
        code, message = yield from self.execute("mail", from_string, *options)

        if code != SMTP_COMPLETED:
            if code == SMTP_NOT_AVAILABLE:
                self.close()
            else:
                # reset, raise error
                try:
                    yield from self.rset()
                except smtplib.SMTPServerDisconnected:
                    pass
            raise smtplib.SMTPSenderRefused(code, message, sender)

        return code, message

    @asyncio.coroutine
    def rcpt(self, recipient, options=None):
        options = options or []
        to_string = "TO:{}".format(quoteaddr(recipient))
        errors = {}
        try:
            code, message = yield from self.execute("rcpt", to_string, *options)
        except smtplib.SMTPResponseException as exc:
            if 520 <= exc.smtp_code <= 599:
                errors[recipient] = (exc.smtp_code, exc.smtp_error)
            else:
                raise exc
        else:
            if code == SMTP_NOT_AVAILABLE:
                yield from self.close()
                errors[recipient] = (code, message)

        if errors:
            raise smtplib.SMTPRecipientsRefused(errors)

        return code, message

    @asyncio.coroutine
    def data(self, message):
        code, response = yield from self.execute("data")
        if code != SMTP_START_INPUT:
            raise smtplib.SMTPDataError(code, response)

        if not isinstance(message, str):
            message = message.decode('utf-8')
        message = re.sub(r'(?:\r\n|\n|\r(?!\n))', CRLF, message)
        message = re.sub(r'(?m)^\.', '..', message)  # quote periods
        if message[-2:] != CRLF:
            message += CRLF
        message += ".%s" % CRLF
        if self.debug:
            logging.debug('message is: %s', message)

        yield from self.send_data(message)

        code, response = yield from self.read_line()
        if code != SMTP_COMPLETED:
            if code == SMTP_NOT_AVAILABLE:
                self.close()
            else:
                # reset, raise error
                try:
                    yield from self.rset()
                except smtplib.SMTPServerDisconnected:
                    pass
            raise smtplib.SMTPDataError(code, response)

        return code, response

    @asyncio.coroutine
    def sendmail(self, from_addr, to_addrs, msg, mail_options=None, rcpt_options=None):
        mail_options = mail_options or []
        rcpt_options = rcpt_options or []

        yield from self.ehlo_or_helo_if_needed()

        esmtp_opts = []
        if isinstance(msg, str):
            msg = _fix_eols(msg).encode('ascii')
        if self.supports_esmtp:
            if self.supports('size'):
                esmtp_opts.append("size=%d" % len(msg))
            for option in mail_options:
                esmtp_opts.append(option)
        try:
            (code, resp) = yield from self.mail(from_addr, esmtp_opts)

        except smtplib.SMTPSenderRefused:
            yield from self.reconnect()
            yield from self.ehlo_or_helo_if_needed()
            (code, resp) = yield from self.mail(from_addr, esmtp_opts)
        if code != 250:
            if code == 421:
                yield from self.close()
            else:
                yield from self.rset()
            raise smtplib.SMTPSenderRefused(code, resp, from_addr)
        senderrs = {}
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        for each in to_addrs:
            (code, resp) = yield from self.rcpt(each, rcpt_options)
            if (code != 250) and (code != 251):
                senderrs[each] = (code, resp)
            if code == 421:
                yield from self.close()
                raise smtplib.SMTPRecipientsRefused(senderrs)
        if len(senderrs) == len(to_addrs):
            yield from self.rset()
            raise smtplib.SMTPRecipientsRefused(senderrs)

        (code, resp) = yield from self.data(msg)
        if code != 250:
            if code == 421:
                yield from self.close()
            else:
                yield from self.rset()
            raise smtplib.SMTPDataError(code, resp)
        # if we got here then somebody got our mail
        return senderrs

    @asyncio.coroutine
    def send_message(self, msg, from_addr=None, to_addrs=None, mail_options=None, rcpt_options=None):
        mail_options = mail_options or []
        rcpt_options = rcpt_options or {}

        yield from self.ehlo_or_helo_if_needed()
        resent = msg.get_all('Resent-Date')
        if resent is None:
            header_prefix = ''
        elif len(resent) == 1:
            header_prefix = 'Resent-'
        else:
            raise ValueError("message has more than one 'Resent-' header block")
        if from_addr is None:
            from_addr = (msg[header_prefix + 'Sender']
                         if (header_prefix + 'Sender') in msg
                         else msg[header_prefix + 'From'])
        if to_addrs is None:
            addr_fields = [f for f in (msg[header_prefix + 'To'],
                                       msg[header_prefix + 'Bcc'],
                                       msg[header_prefix + 'Cc'])
                           if f is not None]
            to_addrs = [a[1] for a in smtplib.email.utils.getaddresses(addr_fields)]
        msg_copy = copy.copy(msg)
        del msg_copy['Bcc']
        del msg_copy['Resent-Bcc']
        international = False

        with io.BytesIO() as bytesmsg:
            if international:
                g = smtplib.email.generator.BytesGenerator(bytesmsg, policy=msg.policy.clone(utf8=True))
                mail_options += ['SMTPUTF8', 'BODY=8BITMIME']
            else:
                g = smtplib.email.generator.BytesGenerator(bytesmsg)
            g.flatten(msg_copy, linesep='\r\n')
            flatmsg = bytesmsg.getvalue()

        result = yield from self.sendmail(from_addr, to_addrs, flatmsg, mail_options, rcpt_options)
        return result
