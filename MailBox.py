import string, secrets, os, re
from transliterate import translit
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Boolean, Column, DateTime, Numeric, SmallInteger, String, Text, text
from sqlalchemy.orm import relationship




roomid = os.environ['MAIL_ROOM_ID'] # '661cceabebdbe25c1ec7a41f'
engine_string = os.environ['ENGINE_STRING'] # postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}
engine = create_engine(engine_string)
Session = sessionmaker(bind=engine)
Base = declarative_base()
metadata = Base.metadata


class Mailbox(Base):
    __tablename__ = 'mailbox'
    __table_args__ = {'comment': 'Postfix Admin - Virtual Mailboxes'}

    username = Column(String(255), primary_key=True)
    password = Column(String(255), nullable=False)
    maildir = Column(String(255), nullable=False)
    quota = Column(Numeric, nullable=False, server_default=text("'0'::numeric"))
    created = Column(DateTime(True), nullable=False, server_default=text("'2000-01-01 00:00:00+00'::timestamp with time zone"))
    modified = Column(DateTime(True), nullable=False, server_default=text("'2000-01-01 00:00:00+00'::timestamp with time zone"))
    active = Column(Boolean, nullable=False, server_default=text("true"))

class Alias(Base):
    __tablename__ = 'alias'
    __table_args__ = {'comment': 'Postfix Admin - Virtual Aliases'}

    address = Column(String(255), primary_key=True)
    goto = Column(Text, nullable=False)
    domain = Column(String(255), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False, server_default=text("'2000-01-01 00:00:00+00'::timestamp with time zone"))
    modified = Column(DateTime(True), nullable=False, server_default=text("'2000-01-01 00:00:00+00'::timestamp with time zone"))
    active = Column(Boolean, nullable=False, server_default=text("true"))


class AddressInfo(Base):
    __tablename__ = 'address_info'

    address = Column(String(255), primary_key=True)
    type = Column(SmallInteger, nullable=False, server_default=text("0"))
    name = Column(String(255), nullable=False)
    surname = Column(String(255))
    patronymic = Column(String(255))
    city = Column(String(255))
    comment = Column(String(255))

class MailMessage:
    def __init__(self, mail_request) -> None:
        self.msg_lines = mail_request.get('msg').split('\n')
        self.mgs_id = mail_request.get('_id')
        self.msg_rid = mail_request.get('rid')
        self.msg_author = mail_request.get('u').get('username')
        alphabet = string.ascii_letters + string.digits
        self.__pass_word = ''.join(secrets.choice(alphabet) for i in range(12))
        self.date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.payload = None
        self.user_name = None
        self.user_email = None
        self.mail_dir = None
        self.comment = ''
        self.success = None
        self.error = None
        with open('acl.yaml', 'r') as stream:
            self.acl = yaml.load(stream)

    def auth(self):
        if self.msg_author in self.acl['email']:
            return True
        else:
            self.error = f'ACL: {self.msg_author} access denied!'
            return False

    def process_request(self):
        payload = {'rid': self.msg_rid,
                   'tmid': self.mgs_id,
                   'msg': f'Info: task accepted..'}
        rocket.chat_send_message(payload)
        if self.auth():
            if self.validate_request():
                self.gen_mail_attributes()
                self.create_email()
                self.reply()
            else:
                self.reply()
        else:
            self.reply()

    def reply(self):
        msg_content = self.success if self.success else self.error
        payload = {'rid': self.msg_rid,
                   'tmid': self.mgs_id,
                   'msg': f'@{self.msg_author}\n{msg_content}'}
        rocket.chat_send_message(payload)

    def gen_mail_attributes(self):
        name = self.payload['name']
        surname = self.payload['surname']
        self.user_name = translit(f"{name.lower()}.{surname.lower()}", 'ru', reversed=True)
        self.user_email = f'{self.user_name}@travelata.ru'
        self.mail_dir = f'travelata.ru/{self.user_name}/'

    def validate_request(self):
        checklist = {
                    'surname': 'фамилия',
                    'name': 'имя',
                    'patronymic': 'отчество',
                    'city': 'город'
                    }
        # ensure that all lines present in request
        result = {}
        for k,v in checklist.items():
            for line in self.msg_lines:
                if v in line:
                    if k in result.keys():
                        self.error = f'Error: duplicate keys: `{k}`.'
                        return False
                    else:
                        clear_line = line.replace(f'{v}:', '').strip()
                        result.update({k: clear_line})
        # ensure that no special symbols present in values
        if len(result) == 4:
            if all([i.isalpha() for i in result.values()]):
                self.payload = result
                return True
            else:
                wrong_values = ' ,'.join([i for i in result.values() if not i.isalpha()])
                self.error = f'Error: invalid sybmol in values: `{wrong_values}`.'
                return False
        else:
            diff = set(checklist.keys()) - set(result.keys())
            diff_keys = ', '.join([checklist[k] for k in diff])
            self.error = f'Error: missing key(s): `{diff_keys}`.'
            return False


    def create_email(self):
        maibox = Mailbox(username=self.user_email,
                         password=self.__pass_word,
                         maildir=self.mail_dir,
                         quota=134221190360,
                         created=self.date_time,
                         modified=self.date_time,)
        
        alias = Alias(address=self.user_email,
                      goto=self.user_email,
                      domain='travelata.ru',
                      created=self.date_time,
                      modified=self.date_time,)

        alias_forward = Alias(address=f'{self.user_name}@travadm.org',
                              goto=self.user_email,
                              domain='travadm.org',
                              created=self.date_time,
                              modified=self.date_time,)
        
        info = AddressInfo(address=self.user_email,
                           type=1,
                           name=self.payload['name'],
                           surname=self.payload['surname'],
                           patronymic=self.payload['patronymic'],
                           city=self.payload['city'],
                           comment=self.comment)

        with Session.begin() as session:
            try:
                session.add_all((maibox, alias, alias_forward, info))
            except Exception as e:
                self.error = f'Create email failed: {e}'
                session.rollback()
            else:
                self.success = f'{self.user_email} {self.__pass_word}'
