#!/usr/bin/env python
# -*- coding: utf-8 -*-
## wbd_downloader.py
## A helpful tool to fetch data from website & generate mdx source file
##
## This program is a free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, version 3 of the License.
##
## You can get a copy of GNU General Public License along this program
## But you can always get it from http://www.gnu.org/licenses/gpl.txt
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
import os
import re
import urllib
import fileinput
import requests
from os import path
from datetime import datetime
from multiprocessing import Pool
from collections import OrderedDict
from lxml import html as Parser


MAX_PROCESS = 20
STEP = 6500
_DEBUG_ = 0
_REVIEW_ = 0
POS = r'\s*\b(?:adj(?:ective\b|\.|\b)|adv(?:erb\b|\.|\b)|pron(?:oun\b|\.|\b)|n(?:oun\b|\.)|(?:(?:in)?transitive\s+)?v(?:erb\b|\.(?:[it]\.)?)|prep(?:osition\b|\.|\b)|conj(?:unction\b|\.|\b)|interj(?:ection\b|\.|\b)|combining form\b)'


def fullpath(file, suffix='', base_dir=''):
    if base_dir:
        return ''.join([os.getcwd(), path.sep, base_dir, file, suffix])
    else:
        return ''.join([os.getcwd(), path.sep, file, suffix])


def readdata(file, base_dir=''):
    fp = fullpath(file, base_dir=base_dir)
    if not path.exists(fp):
        print("%s was not found." % fp)
    else:
        fr = open(fp, 'rU')
        try:
            return fr.read()
        finally:
            fr.close()
    return None


def dump(data, file, mod='w'):
    fname = fullpath(file)
    fw = open(fname, mod)
    try:
        fw.write(data)
    finally:
        fw.close()


def removefile(file):
    if path.exists(file):
        os.remove(file)


def info(l, s='word'):
    return '%d %ss' % (l, s) if l>1 else '%d %s' % (l, s)


def getwordlist(file, base_dir='', tolower=True):
    words = readdata(file, base_dir)
    wordlist = OrderedDict()
    if words:
        p = re.compile(r'\s*\n\s*')
        words = p.sub('\n', words).strip()
        for word in words.split('\n'):
            if tolower:
                wordlist[word.lower()] = None
            else:
                wordlist[word] = None
    else:
        print("%s: No such file or file content is empty." % file)
    return wordlist


class downloader:
#common logic
    def __init__(self, name):
        self.DIC_T = name

    def getpage(self, link, BASE_URL=''):
        r = self.session.get(''.join([BASE_URL, link]), timeout=10, allow_redirects=False)
        if r.status_code == 200:
            return r.content
        else:
            return None

    def cleansp(self, text):
        p = re.compile(r'\s{2,}')
        text = p.sub(' ', text)
        p = re.compile(r'<!--[^<>]+?-->')
        text = p.sub('', text)
        p = re.compile(r'\s*<br/?>\s*', re.I)
        text = p.sub(r'<br>', text)
        p = re.compile(r'(\s*<br>\s*)*(<(?:/?(?:div|p|ul|ol|li|fieldset|table)[^>]*|br)>)(\s*<br>\s*)*', re.I)
        text = p.sub(r'\2', text)
        p = re.compile(r'\s*(<(?:/?(?:div|p|ul|ol|li|fieldset|table)[^>]*|br)>)\s*', re.I)
        text = p.sub(r'\1', text)
        p = re.compile(r'(?<=[^,])\s+(?=[,;\?\!]|\.(?:[^\d\.]|$))')
        text = p.sub(r'', text)
        return text

    def getword(self, file, base_dir=''):
        line = readdata(file, base_dir)
        if line:
            p = re.compile(r'\s*\n\s*')
            line = p.sub('\n', line).strip()
            if line.find('\t')>0:
                word, url = line.split('\t')
            else:
                word, url = line, None
            return word, url
        print("%s: No such file or file content is empty." % file)
        return '', None

    def getcreflist(self, file, base_dir=''):
        words = readdata(file, base_dir)
        if words:
            p = re.compile(r'\s*\n\s*')
            words = p.sub('\n', words).strip()
            crefs = OrderedDict()
            for word in words.split('\n'):
                k, v = word.split('\t')
                crefs[k.strip().rstrip(',').lower()] = v.strip()
            return crefs
        print("%s: No such file or file content is empty." % file)
        return OrderedDict()

    def __mod(self, flag):
        return 'a' if flag else 'w'

    def __dumpwords(self, sdir, words, sfx='', finished=True):
        f = fullpath('rawhtml.txt', sfx, sdir)
        if len(words):
            mod = self.__mod(sfx)
            fw = open(f, mod)
            try:
                [fw.write('\n'.join([en[0], en[1], '</>\n'])) for en in words]
            finally:
                fw.close()
        elif not path.exists(f):
            fw = open(f, 'w')
            fw.write('\n')
            fw.close()
        if sfx and finished:
            removefile(fullpath('failed.txt', '', sdir))
            l = -len(sfx)
            cmd = '\1'
            nf = f[:l]
            if path.exists(nf):
                msg = "Found rawhtml.txt in the same dir, delete?(default=y/n)"
                cmd = 'y'#raw_input(msg)
            if cmd == 'n':
                return
            elif cmd != '\1':
                removefile(nf)
            os.rename(f, nf)

    def __fetchdata_and_make_mdx(self, arg, part, suffix=''):
        sdir, d_app = arg['dir'], OrderedDict()
        words, crefs, count, failed = [], OrderedDict(), 1, []
        leni = len(part)
        while leni:
            for cur in part:
                if count % 100 == 0:
                    print ".",
                    if count % 500 == 0:
                        print count,
                try:
                    page = self.getpage(self.makeurl(cur), self.base_url)
                    if page:
                        if self.makeword(page, cur, words, d_app):
                            crefs[cur] = cur
                            count += 1
                        else:
                            failed.append(cur)
                    else:
                        failed.append(cur)
                except Exception, e:
                    import traceback
                    print traceback.print_exc()
                    print "%s failed, retry automatically later" % cur
                    failed.append(cur)
            lenr = len(failed)
            if lenr >= leni:
                break
            else:
                leni = lenr
                part, failed = failed, []
        print "%s browsed" % info(count-1),
        if crefs:
            mod = self.__mod(path.exists(fullpath('cref.txt', base_dir=sdir)))
            dump(''.join(['\n'.join(['\t'.join([k, v]) for k, v in crefs.iteritems()]), '\n']), ''.join([sdir, 'cref.txt']), mod)
        if d_app:
            mod = self.__mod(path.exists(fullpath('appd.txt', base_dir=sdir)))
            dump(''.join(['\n'.join(d_app.keys()), '\n']), ''.join([sdir, 'appd.txt']), mod)
        if failed:
            dump('\n'.join(failed), ''.join([sdir, 'failed.txt']))
            self.__dumpwords(sdir, words, '.part', False)
        else:
            print ", 0 word failed"
            self.__dumpwords(sdir, words, suffix)
        if self.logs:
            mod = self.__mod(path.exists(fullpath('log.txt', base_dir=sdir)))
            dump('\n'.join(self.logs), ''.join([sdir, 'log.txt']), mod)
        return len(crefs), d_app

    def start(self, arg):
        import socket
        socket.setdefaulttimeout(120)
        import sys
        reload(sys)
        sys.setdefaultencoding('utf-8')
        sdir = arg['dir']
        fp1 = fullpath('rawhtml.txt.part', base_dir=sdir)
        fp2 = fullpath('failed.txt', base_dir=sdir)
        fp3 = fullpath('rawhtml.txt', base_dir=sdir)
        if path.exists(fp1) and path.exists(fp2):
            print ("Continue last failed")
            failed = getwordlist('failed.txt', sdir).keys()
            return self.__fetchdata_and_make_mdx(arg, failed, '.part')
        elif not path.exists(fp3):
            print ("New session started")
            return self.__fetchdata_and_make_mdx(arg, arg['alp'])

    def combinefiles(self, dir):
        times = 0
        for d in os.listdir(fullpath(dir)):
            if re.compile(r'^\d+$').search(d) and path.isdir(fullpath(''.join([dir, d, path.sep]))):
                times += 1
        for fn in ['cref.txt', 'log.txt']:
            fw = open(fullpath(''.join([dir, fn])), 'w')
            for i in xrange(1, times+1):
                sdir = ''.join([dir, '%d'%i, path.sep])
                if path.exists(fullpath(fn, base_dir=sdir)):
                    fw.write('\n'.join([readdata(fn, sdir).strip(), '']))
            fw.close()
        words, phvs = [], []
        crefs = self.load_creflist('cref.txt', dir)
        dump('\n'.join(crefs.keys()), ''.join([dir, 'wordlist.txt']))
        print "Formatting files..."
        self.load_correct_info()
        patch, supp = self.load_patch(dir), self.load_supp_words(dir)
        crefs.update([(k, k) for k in patch.keys()])
        crefs.update([(k, k) for k in supp.keys()])
        pool, args = Pool(25 if times>25 else times), []
        for i in xrange(1, times+1):
            args.append((self, ''.join([dir, '%d'%i, path.sep])))
        dics = pool.map(formatter, args)
#        for i in xrange(1, times+1):#For debug
#            formatter((self, ''.join([dir, '%d'%i, path.sep])))#For debug
        print "Start to combine files at %s" % datetime.now()
        illu = self.load_illustrations()
        fm = ''.join([dir, self.DIC_T, path.extsep, 'txt'])
        fw = open(fullpath(fm), 'w')
        try:
            for i in xrange(1, times+1):
                sdir = ''.join([dir, '%d'%i, path.sep])
                file = fullpath('formatted.txt', base_dir=sdir)
                lns = []
                for ln in fileinput.input(file):
                    ln = ln.strip()
                    if ln == '</>':
                        word = lns[0].rstrip(' ,')
                        if word in supp:
                            if supp[word]:
                                fw.write(self.refine(word, supp[word], illu, phvs))
                                supp[word] = None
                        elif lns[1]=='###' or word in patch:
                            if not word in patch:
                                print "Need patch: %s" % word
                            if word in patch:
                                fw.write(self.refine(word, patch[word], illu, phvs))
                                del patch[word]
                            elif not _DEBUG_:
                                self.logs.append('E01:\t%s: patch was not found'%word)
                        else:
                            fw.write(self.refine(word, lns[1], illu, phvs))
                        words.append(word)
                        del lns[:]
                    elif ln:
                        lns.append(ln)
                os.remove(file)
        finally:
            fw.close()
        if patch:
            dump(''.join([self.refine(k, v, illu, phvs) for k, v in patch.iteritems()]), fm, 'a')
            words.extend(patch.keys())
        lns = []
        for k, v in supp.iteritems():
            if v:
                lns.append(self.refine(k, v, illu, phvs))
                words.append(k)
        if lns:
            dump(''.join(lns), fm, 'a')
        print "%s " % info(len(words)),
        dump('\n'.join(words), ''.join([dir, 'words.txt']))
        lns = self.uni_phvs(phvs, OrderedDict([(w.lower(), None) for w in words]), dir)
        if lns:
            print "and %s " % info(len(lns), 'links & phrase'),
            dump(''.join(lns), fm, 'a')
        print "totally."
        if illu:
            print "There are no corresponding words for the following illustrations:"
            print "\n".join(['\t'.join([k, ', '.join([', '.join([v[1] for v in d[s]]) for s in d])]) for k, d in illu.iteritems()])
        for dic in dics:
            self.logs.extend(dic.logs)
            if _REVIEW_ or _DEBUG_:
                self.hc_d.update(dic.hc_d)
                self.lang_cr.update(dic.lang_cr)
                self.lang_d.update(dic.lang_d)
                self.lang_d2.update(dic.lang_d2)
                self.img_d.update(dic.img_d)
        if self.logs:
            mod = self.__mod(path.exists(fullpath('log.txt', base_dir=dir)))
            dump('\n'.join(self.logs), ''.join([dir, 'log.txt']), mod)
        if _REVIEW_ or _DEBUG_:
            dump('\n'.join(['\n'.join(['\t'.join([w, k]) for w in v]) for k, v in self.hc_d.iteritems()]), 'h.txt')
            dump('\n'.join(['\n'.join(['\t'.join([k, wr, rt, f]) for k, f, rt in v]) for wr, v in self.lang_cr.iteritems()]), 'la_cr.txt')#
            dump('\n'.join(['\t'.join([k, '\t'.join(v)]) for k, v in self.lang_d.iteritems()]), 'la.txt')
            dump('\n'.join(['\t'.join([k, '\t'.join(v)]) for k, v in self.lang_d2.iteritems()]), 'la2.txt')
            dump('\n'.join(self.img_d.values()), 'img.txt')


def formatter((dic, sdir)):
    lns, fmtd = [], []
    file = fullpath('rawhtml.txt', base_dir=sdir)
    for ln in fileinput.input(file):
        ln = ln.strip()
        if ln == '</>':
            word = lns[0]
            if word in dic.words_need_split:
                entry = dic.split_entry(word, lns[1])
                if entry:
                    fmtd.append(''.join([entry, '\n']))
            elif word in dic.words_need_extr:
                fmtd.append(''.join([dic.extr_entry(word, lns[1]), '\n']))
            else:
                fmtd.append(''.join([dic.format_entry(word, lns[1]), '\n']))
            del lns[:]
        elif ln:
            lns.append(ln)
    dump('\n'.join(fmtd), ''.join([sdir, 'formatted.txt']))
    print sdir
    return dic


def f_start((obj, arg)):
    return obj.start(arg)


def multiprocess_fetcher(dir, d_refs, wordlist, obj, base):
    times = int(len(wordlist)/STEP)
    pl = [wordlist[i*STEP: (i+1)*STEP] for i in xrange(0, times)]
    pl.append(wordlist[times*STEP:])
    times = len(pl)
    fdir = fullpath(dir)
    if not path.exists(fdir):
        os.mkdir(fdir)
    imgpath = fullpath(''.join([dir, 'p', path.sep]))
    if not path.exists(imgpath):
        os.mkdir(imgpath)
    for i in xrange(1, times+1):
        subdir = ''.join([dir, '%d'%(base+i)])
        subpath = fullpath(subdir)
        if not path.exists(subpath):
            os.mkdir(subpath)
    pool, n = Pool(MAX_PROCESS), 1
    d_app = OrderedDict()
    while n:
        args = []
        for i in xrange(1, times+1):
            sdir = ''.join([dir, '%d'%(base+i), path.sep])
            file = fullpath(sdir, 'rawhtml.txt')
            if not(path.exists(file) and os.stat(file).st_size):
                param = {}
                param['alp'] = pl[i-1]
                param['dir'] = sdir
                args.append((obj, param))
        if len(args) > 0:
            vct = pool.map(f_start, args)#f_start(args[0])#for debug
            n = 0
            for count, dt in vct:
                n += count
                d_app.update(dt)
        else:
            break
    dt = OrderedDict()
    for k, v in d_app.iteritems():
        if not k in d_refs:
            dt[k] = v
    return times, dt.keys()


class wbd_downloader(downloader):
#WBD downloader
    def __init__(self):
        downloader.__init__(self, 'WBD')
        self.__origin = 'http://www.worldbookonline.com'
        self.__base_url = ''.join([self.__origin, '/advanced/'])
        self.__logs = []
        self.__re_d = {re.I: {}, 0: {}}
        if _REVIEW_ or _DEBUG_:
            self.hc_d = OrderedDict()
            self.lang_d = OrderedDict()
            self.lang_d2 = OrderedDict()
            self.lang_cr = OrderedDict()
            self.img_d = OrderedDict()

    @property
    def base_url(self):
        return self.__base_url

    @property
    def logs(self):
        return self.__logs

    @property
    def words_need_split(self):
        return self.__words_need_split

    @property
    def words_need_extr(self):
        return self.__words_need_extr

    @property
    def session(self):
        return self.__session

    def login(self):
        HEADER = 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'
        url = ''.join([self.__origin, '/wb/Home'])
        param = {'auth': 'UidPassword', 'isAjax': 'false', 'uid': '***',
        'pwd': '***', 'x': '7', 'y': '7', 'cardno': ''}
        self.__session = requests.Session()
        self.__session.headers['User-Agent'] = HEADER
        self.__session.headers['Origin'] = self.__origin
        self.__session.headers['Referer'] = ''.join([self.__origin, '/wb/Login?ed=wb'])
        r = self.__session.post(url, data=param)
        if r.status_code != 404:
            print self.__session.cookies
            self.__session.headers['Referer'] = self.__base_url
        else:
            print r.text
            self.__session = None

    def __preformat(self, page):
        page = page.replace('\xC2\xA0', ' ').replace('&#160;', ' ').replace('&#160', ' ')
        p = re.compile(r'[\n\r]+(\s+[\n\r]+)?')
        page = p.sub('\xFF\xFF', page)
        n = 1
        while n:
            p = re.compile(r'\t+|&(?:nb|en|em|thin)sp;|\s{2,}')
            page, n = p.subn(r' ', page)
        p = re.compile(r'(</?)strong(?=[^>]*>)')
        page = p.sub(r'\1b', page)
        return page

    def __ref2a(self, uc):
        return re.compile('\xE2\x80[\x99\x9A]').sub('\'', uc)

    def makeurl(self, cur):
        return ''.join(['dictionary?lu=', self.__ref2a(cur).replace('/', '\''), '&cl=2'])

    def __locz_img(self, m):
        rpath, fnm = m.group(2), m.group(3)
        file = ''.join([self.DIC_T, path.sep, 'p', path.sep, fnm])
        if not path.exists(fullpath(file)):
            dump(self.getpage(''.join([rpath, fnm]), self.__origin), file, 'wb')
        return ''.join([m.group(1), 'p/', fnm])

    def __update_links(self, m, word, d_app):
        if m:
            asd = m.group(1)
            p = re.compile(r'([\(\)\.\[\]\*\+\?\|])')
            rw = p.sub(r'\\\1', word).replace('\'', '(?:\xE2\x80[\x99\x9A]|\')')
            p = re.compile(''.join([r'<a\s+[^<>]*?href="#"\s*onclick=[^<>]+>\s*', rw, r'\s*</a>']), re.I)
            m = p.search(asd)
            if m:
                p = re.compile(r'<a\s+[^<>]*?href="#"\s*onclick="showDictionary\(\'\s*([^<>]+)\s*\'\)">\s*(.+?)\s*</a>', re.I)
                for link, word in p.findall(asd):
                    d_app[self.__ref2a(word)] = None
                return True
        self.__logs.append('W01:\t%s -> wrong link'%word)
        return False

    def __repimg(self, page):
        p = re.compile(r'(<img [^<>]*?src=")\s*(/[^<>"]+/)([^<>"]+?)\s*(?="[^<>]*>)', re.I)
        page = p.sub(self.__locz_img, page)
        assert not re.compile(r'<img [^<>]*?src="\s*/', re.I).search(page)
        return page

    def makeword(self, page, cur, words, d_app):
        page = self.__preformat(page)
        word = self.__ref2a(cur).replace('&amp;', '&')
        p = re.compile(r'<aside [^<>]+>(.+?)</aside>', re.I)
        if self.__update_links(p.search(page), cur, d_app):
            p = re.compile(r'<A NAME="ent_[^<>"]+">\s*</A>', re.I)
            if p.search(page):
                p = re.compile(r'<div class="dictonart-content"[^<>]*>[\xFF\s]*<ul class="main-search-result">[\xFF\s]*(.+?)[\xFF\s]*</div>[\xFF\s]*(?=<!--Search Result ENDS-->|$)', re.I)
                words.append([word, self.__repimg(p.search(page).group(1))])
            else:
                self.__logs.append('W02:\t"%s" has no content'%word)
                words.append([word, '###'])
            return True
        else:
            return False

    def __rex(self, ptn, mode=0):
        if ptn in self.__re_d[mode]:
            pass
        else:
            self.__re_d[mode][ptn] = re.compile(ptn, mode) if mode else re.compile(ptn)
        return self.__re_d[mode][ptn]

    def load_creflist(self, file, base_dir=''):
        self.__crefs = self.getcreflist(file, base_dir)
        return self.__crefs

    def load_patch(self, base_dir=''):
        patch = OrderedDict()
        pt_file = fullpath(''.join([base_dir, 'links.txt']))
        if path.exists(pt_file):
            lns = []
            for ln in fileinput.input(pt_file):
                ln = ln.strip()
                if ln == '</>':
                    patch[lns[0]] = lns[1]
                    del lns[:]
                elif ln:
                    lns.append(ln)
        return patch

    def load_supp_words(self, base_dir=''):
        supp = OrderedDict()
        dir = ''.join([base_dir, 'supplement', path.sep])
        for root, dirs, files in os.walk(fullpath(dir)):
            for file in files:
                text = self.__preformat(readdata(file, dir))
                p = re.compile(r'<FONT class="dictionary">[\xFF\s]*(.+?)(?=<div id="dictionaryOverLay"|$)', re.I)
                m = p.search(text)
                if m:
                    text = m.group(1)
                key = urllib.unquote(path.splitext(file)[0])
                supp[key] = self.format_entry(key, self.__repimg(text), False)
        return supp

    def __getlimit(self, w, h):
        w, h = w*0.6, h*0.6
        if w/310 >= h/400:
            return ''.join([' width=', str(int(w))])
        else:
            return ''.join([' height=', str(int(h))])

    def load_illustrations(self):
        illu, info = OrderedDict(), {}
        text = readdata('img_info.txt')
        if text:
            text = re.compile(r'\n+(\s+\n+)?').sub('\n', text).strip('\n')
            for img in text.split('\n'):
                k, w, h = img.split('\t')
                info[k] = self.__getlimit(int(w), int(h))
        dir = ''.join(['data', path.sep, 'p', path.sep])
        if path.exists(dir):
            for root, dirs, files in os.walk(fullpath(dir)):
                for file in files:
                    name, ext = path.splitext(file)
                    if re.compile(r'^(?:png|jpg|jpeg|gif|bmp|ico)$', re.I).search(ext.strip().lstrip(path.extsep)):
                        name = urllib.unquote(name).strip().lower()
                        p = self.__rex(r'^(.+?)(\d?)(?:_(\d))?$')
                        m = p.search(name)
                        name, sup, idx = m.group(1), m.group(2), m.group(3)
                        limit = info[file] if file in info else ''
                        if name in illu:
                            if sup in illu[name]:
                                illu[name][sup].append((idx, file, limit))
                            else:
                                illu[name][sup] = [(idx, file, limit)]
                        else:
                            illu[name] = {sup: [(idx, file, limit)]}
        return illu

    def __fmt_rt(self, rt):
        rt = rt.replace('&#x0231;', '&#x022F;&#x0304;')
        rt = re.compile(r'(\w(?:&#x030[2467];){1,2}|&#x\w{3,4};&#x030[14];)').sub(r'<span class="cwi">\1</span>', rt)
        return rt

    def load_correct_info(self, base_dir=''):
        self.__words_need_split = OrderedDict([(k, None) for k in [
        'ant', '-off', 'over', 'out', 'ultra', 'we\'re', 'trp', 'troy weight',
        'apothecary', 'apothecaries\' measure', 'apothecaries\' weight',
        'a volonte', 'avoirdupois weight', 'chain molding', 'chain measure',
        'circular mil', 'circular measure', 'cubicule', 'cubic measure',
        'ditto mark', 'ditty', 'dry milk', 'dry measure', 'linear motor',
        'linear measure', 'liquid membrane', 'liquid measure', 'surview',
        'surveyor\'s measure', 'square mile', 'square measure']])
        self.__words_need_extr = OrderedDict([(k, None) for k in [
        'un', 'out-', 'over-', 'pre-', 'ultra-', 'non', 're']])
        self.__trans_tbl = {'&#160;': ' ', '&#160': ' ', '&#034;': '"',
        '&#37;': '%', '&#38;': '&amp;', '&#038;': '&amp;', '&#39;': '\'',
        '&#060;': '&lt;', '&#062;': '&gt;',
        '&#133;': '&hellip;', '&#136;': '&circ;', '&#145;': '&lsquo;',
        '&#146;': '&rsquo;', '&#147;': '&ldquo;', '&#148;': '&rdquo;',
        '&#149;': '&bull;', '&#150;': '&ndash;', '&#151;': '&mdash;',
        '&#162;': '&cent;', '&#163;': '&pound;', '&#166;': '&brvbar;',
        '&#167;': '&sect;', '&#171;': '&laquo;', '&#173;': '-',
        '&#176;': '&deg;', '&#180;': '&acute;', '&#187;': '&raquo;',
        '&#188;': '&frac14;', '&#189;': '&frac12;', '&#190;': '&frac34;',
        '&#191;': '&iquest;', '&#243;': '&oacute;', '&#247;': '&divide;'}
        self.__trans_p = re.compile('|'.join(self.__trans_tbl.keys()))
        self.__right_h = ['&prime;', '&Prime;', '&part;', '&euml;', '&ne;',
        '&agrave;', '&ecirc;', '&apos;', '&eacute;', '&acirc;', '&egrave;',
        '&ntilde;', '&times;', '&radic;', '&rsquo;', '&rarr;', '&equiv;',
        '&ldquo;', '&rdquo;']
        self.__pos_tbl = {'adj': 'adjective', 'adv': 'adverb', 'n': 'noun',
        'v.i': 'intransitive verb', 'v.t': 'transitive verb', 'v': 'verb',
        'prep': 'preposition', 'conj': 'conjunction', 'interj': 'interjection',
        'pron': 'pronoun', 'n.pl': 'noun, <em>plural</em>'}
        self.__uc_tbl = {'&#x00E1;': 'a', '&#x00E8;': 'e', '&#x00E9;': 'e',
        '&#x00F1;': 'n', '&#x00E7;': 'c', '&#x00E5;': 'a', '&#x00E4;': 'a',
        '&#x00FC;': 'u', '&#x00F6;': 'o', '&#x00E2;': 'a', '&#x00F3;': 'o',
        '&#x00EA;': 'e', '&#x00E0;': 'a', '&#x00EB;': 'e'}
        if _DEBUG_:
            self.__spell_tbl = {}
        words = readdata('correct.txt', base_dir)
        self.__correct_list = OrderedDict()
        self.__chrimg_list = OrderedDict()
        if words:
            p = re.compile(r'\n+(\s+\n+)?')
            words = p.sub('\n', words).strip('\n')
            for word in words.split('\n'):
                k, wr, rt = word.split('\t')
                if k.startswith('(?:'):
                    kl = k[3:-1].split('|')
                else:
                    kl = [k]
                for k in kl:
                    if re.compile(r'^\s*@').search(k):
                        self.__chrimg_list[wr] = self.__fmt_rt(rt)
                    elif not re.compile(r'^\s*//').search(k):
                        rt = self.__fmt_rt(rt)
                        if k in self.__correct_list:
                            self.__correct_list[k].append((wr, rt))
                        else:
                            self.__correct_list[k] = [(wr, rt)]
        else:
            print "Correct list was not found."

    def __corol(self, m):
        ol = m.group(1)
        pos = ol.rfind('<li>')
        t1, t2 = ol[:pos], ol[pos:]
        p = self.__rex(r'\s*(<p>\s*<a name="ety_[^<>"]+">\s*</a>.+?)\s*(</li>\s*</ol>)', re.I)
        t2 = p.sub(r'\2\1', t2)
        return ''.join([t1, t2])

    def __corety(self, m):
        a, ety = m.group(1), m.group(2)
        ety = self.__rex(r'^L?B>').sub('[', ety)
        ety = self.__rex(r'R?B>$').sub(']', ety)
        lbc, rbc = ety.count('['), ety.count(']')
        p = self.__rex('</?(?:BR|ol|p|li|div|hr)[^<>]*>|<span class="wb-dict-headword">', re.I)
        if lbc==0 and rbc==1 and ety.startswith('&lt;'):
            ety = ''.join(['[', ety])
        elif lbc==1 and rbc==0 and not p.search(ety):
            ety = ''.join([ety.rstrip(), ']'])
        return ''.join([a, ety])

    def __mk_ref(self, ref, word):
        xref = self.__rex(r'\W+$').sub('', ref)
        if xref.lower() in self.__crefs:
            return xref
        xref = self.__rex(r'(?<=\w)(?=s\b)').sub('\'', ref)
        if xref.lower() in self.__crefs:
            return xref
        p = self.__rex(r'(\w+)(&[^<>]+;)')
        xref = ref
        for n in p.finditer(word):
            xref = xref.replace(n.group(1), n.group(0), 1)
        xref = self.__mk_sk(xref, True)
        if xref.lower() in self.__crefs:
            return xref
        xref = ''.join([ref, '.'])
        if xref.lower() in self.__crefs:
            return xref
        p = self.__rex(r'(\w+)(-)')
        n = p.search(word)
        if n:
            xref = self.__mk_sk(ref.replace(n.group(1), n.group(0), 1), True)
            if xref.lower() in self.__crefs:
                return xref
        p = self.__rex(r'^\s*-(\w+)')
        xref = p.sub(r'\1', ref)
        if xref.lower() in self.__crefs:
            return xref
        p = self.__rex(r'(\w+)-(\w+)')
        xref = p.sub(r'\1 \2', ref)
        if xref.lower() in self.__crefs:
            ref = xref
        return ref

    def __chk_ref(self, m, key):
        word = m.group(3)
        n = 1
        while n:
            p = self.__rex(r'((?:<SUP>|\(def(?:[si]|\b)|(?:\(|,?\s*)<i>).*?)(</B>)', re.I)
            word, n = p.subn(r'\2\1', word)
        word = self.__rex(r'<B>\s*</B>', re.I).sub('', word)
        p = self.__rex(r'(\s*(?:(?:(?:<SUP>|\(def(?:[si]|\b)|(?:\(|,?\s*)<i>).*?)|\.\s*))(</a>)', re.I)
        word =p.sub(r'\2\1', word)
        p = self.__rex(r'(?<=</a>)\s+(?=<SUP>)', re.I)
        word = p.sub('', word)
        ref, chk = self.__mk_sk(m.group(1).replace('\\\'', '\''), True), False
        if not ref.strip().lower() in self.__crefs:
            i1, i2 = ref.find('('), word.find('(')
            if i1 > 0:
                xref = ref[:i1].strip()
                if xref.lower() in self.__crefs:
                    ref, chk = xref, True
                    ia = word.find('</a>')
                    if i2 < ia:
                        word = ''.join([word[:i2].rstrip(), '</a> ', word[i2:ia], word[ia+4:]])
            if not chk:
                ref = self.__mk_ref(ref, word)
        if _DEBUG_:
            if not ref.lower() in self.__crefs:
                self.__logs.append('E02:\tx-ref not found\t%s\t%s\t%s'%(key, m.group(0), m.group(1)))
        return ''.join([ref.replace('/', '%2F'), m.group(2), word])

    def __correct_data(self, key, line):
        p = self.__rex(r'(?<=\w)&verbar;(?=\w)')
        line = p.sub(r'', line)
        if _DEBUG_:
            p = self.__rex(r'&[^#][^;\s]+[\W\s]|&#x?\w+[^\w;]|&#x00[0-7]\w[^\w]|&#x\d{1,2}[^\w]')
            hcs = p.findall(line)
            for h in hcs:
                if h not in self.__right_h:
                    if h in self.hc_d:
                        if not key in self.hc_d[h]:
                            self.hc_d[h].append(key)
                    else:
                        self.hc_d[h] = [key]
        if key in self.__correct_list:
            for wr, rt in self.__correct_list[key]:
                if wr.startswith('$'):
                    line = self.__rex(wr[1:], re.I).sub(rt, line)
                else:
                    line = line.replace(wr, rt)
        line = self.__trans_p.sub(lambda m: self.__trans_tbl[m.group(0)], line)
        if _DEBUG_:
            for wr, rt in self.__spell_tbl.iteritems():
                p = self.__rex(wr, re.I)
                for f in p.findall(line):
                    line = line.replace(f, rt)
                    if wr in self.lang_cr:
                        self.lang_cr[wr].append((key, f, rt))
                    else:
                        self.lang_cr[wr] = [(key, f, rt)]
        p = self.__rex(r'&#\d+')
        assert not p.search(line)
        p = self.__rex(r'<span class="wb-dict-headword"><B>(\W*)</B></span>', re.I)
        line = p.sub(r'\1', line)
        p = self.__rex(r'(?<=<ol>)(.+?</ol>)', re.I)
        line = p.sub(self.__corol, line)
        p = self.__rex(r'(?<=<a href=")javascript:showEntry\(\'\s*([^<>]+?)\',\s*\'\s*(ent_[^<>]+?)\'\)" target="_top"\.?(?=[\xFF\s]*(?:</li|</?p>|<BR|<A NAME="def_[^<>"]+">))', re.I)
        line = p.sub(lambda m: ''.join(['entry://',
        self.__mk_sk(m.group(1), True), '#', m.group(2), '"></a>']), line)
        p = self.__rex(r'\s*(<a name="ety_[^<>"]+">\s*</a>)[\xFF\s]*(.+?)[\xFF\s]*(?=</?(?:BR|ol|p)\b)', re.I)
        line = p.sub(self.__corety, line)
        n = 1
        while n:
            p = self.__rex(r'((?:[\xFF\s]|<BR>)*<NOBR>\s*&ndash;\s*<span class="wb-dict-headword">[^\xFF]+?</span>\s*</NOBR>[^\xFF]*?)[\xFF\s]*(</li></ol>)', re.I)
            line, n =p.subn(r'\2\1', line)
        p = self.__rex(r'<a href="javascript:showEntry\(\'\s*\',[^<>]+>(.*?)</a>', re.I)
        line = p.sub(r'\1', line)
        p = self.__rex(r'(?<=<a href="javascript:showEntry\(\')\s*([^<>]+?)(\',\s*\'\s*ent_[^<>]+>)(.*?</a>)', re.I)
        line = p.sub(lambda m: self.__chk_ref(m, key), line)
        p = self.__rex(r'(<A NAME="def_[^<>]+"></A><FONT COLOR="#404040"><B>a</B></FONT>[\xFF\s]*)</?BR>', re.I)
        line = p.sub(r'\1', line)
        p = self.__rex(r'([^\.][a-z])(\.\d+)', re.I)
        line = p.sub(r'\1 \2', line)
        p = self.__rex(r'<DIV ALIGN="CENTER">(.+?)</DIV>', re.I)
        line = p.sub(r'\1', line)
        return line

    def __trans_lower(self, m):
        tag = m.group(2)
        p = self.__rex(r'^\w+', re.I)
        tag = p.sub(lambda n: n.group(0).lower(), tag)
        p = self.__rex('([\w\-]+)\s*(?=\=\s*["\'])', re.I)
        tag = p.sub(lambda n: n.group(1).lower(), tag)
        p = self.__rex('(align\s*=\s*[\'"])([\w\s-]+)', re.I)
        tag = p.sub(lambda n: ''.join([n.group(1), n.group(2).lower()]), tag)
        return ''.join([m.group(1), tag])

    def __tag2lower(self, line):
        p = self.__rex(r'(</?)\s*([^<>]+)\s*(?=>)', re.I)
        return p.sub(self.__trans_lower, line)

    def __rep_hd(self, hd):
        p = self.__rex(r'(?<=</B>)\s*\|\s*(<B)(?=>)', re.I)
        hd = p.sub(r'\1 class="ykc"', hd)
        p = self.__rex(r'(?<=<B)>&acute;(?=</B>)', re.I)
        hd = p.sub(' class="bdt">', hd)
        p = self.__rex(r'(</B>\s*)&acute;(?=\s*<B>)', re.I)
        hd = p.sub(r'\1<em class="t1u"></em>', hd)
        p = self.__rex(r'</?NOBR>', re.I)
        return p.sub('', hd)

    def __rep_pr(self, pr):
        p = self.__rex(r'&laquo;\s*(.+?)\s*&raquo;', re.I)
        pr = p.sub(r'<span class="zxm">\1</span>', pr)
        p = self.__rex(r'<SMALL>(\s*)</SMALL>', re.I)
        pr = p.sub(r'\1', pr)
        return pr

    def __rep_rg(self, rg, lpad=' ', rpad=' '):
        p = self.__rex(r'^((?:especially|chiefly|in(?: the)?|now)\s+)?[A-Z]')
        if p.search(rg):
            return ''.join([lpad, '<span class="rg7">', rg, '</span>', rpad])
        else:
            return None

    def __reprgs(self, m, lpad=' ', rpad=' '):
        rg = self.__rep_rg(m.group(1), lpad, rpad)
        return rg if rg else m.group(0)

    def __reprgs2(self, m, lpad=' ', rpad=' '):
        rg = self.__rep_rg(m.group(2), lpad, rpad)
        return ''.join([m.group(1), rg]) if rg else m.group(0)

    def __rep_pt(self, pos):
        p = self.__rex(''.join([r'(\s*)(', POS, r')']))
        pos =p.sub(r'\1<span class="kce">\2</span>', pos)
        p = self.__rex(r'([,\.;\(\s]*)(\w[^<>]+?)(?=\s*<span|$)', re.I)
        pos = p.sub(lambda n: ''.join([n.group(1),
        '<i>', self.__rex(r'(\s*[\(\)]\s*)').sub(r'</i>\1<i>', n.group(2)), '</i>']), pos).replace('<i></i>', '')
        p = self.__rex(r'\b(sing)\b', re.I)
        pos = p.sub(r'\1ular', pos)
        return pos

    def __repps(self, n):
        pos = ''.join(['<span class="kce">', n.group(1), '</span>', self.__rep_pt(n.group(2)), n.group(3)])
        p = self.__rex('</(span>\W*<)span class="kce">', re.I)
        return p.sub(r'<\1/span>', pos)

    def __rep_ps(self, pos):
        p = self.__rex(''.join([r'<i>(', POS, r')([^<>]*?)([,\.;\)\s]*)</i>']))
        return p.sub(self.__repps, pos)

    def __repinfl(self, m, key):
        pron = self.__rep_hd(m.group(1)).replace('\xFF\xFF', '')
        if not pron.strip():
            return m.group(0)
        p = self.__rex(r'(?<=<span class="zxm">)(.+?)(?=</span>)', re.I)
        q1 = self.__rex(r'(</?)i(?=>)', re.I)
        q2 = self.__rex(r'(?<=;)\s+')
        pron = p.sub(lambda n: q2.sub(r'<b> </b>', q1.sub(r'\1em', n.group(1))), pron)
        p = self.__rex(r'(?<=</span>)(.+?)$', re.I)
        q = self.__rex(r'\s*<i>\s*\W?\s*<i>\s*([^<>]{3,}?)\s*</i>\s*\W?\s*</i>\s*', re.I)
        pron = q.sub(self.__reprgs, pron)
        p = self.__rex(r'(?<=<i>)(n\b|v(?=</i>|\.\W))')
        pron = p.sub(self.__trans_pos, pron)
        pron = self.__rep_ps(pron)
        p = self.__rex(r'<i>((?:Possessive|Objective)\b[^<>]+)</i>')
        pron = p.sub(r'<em>\1</em>', pron)
        p = self.__rex(r'<i>\s*([^<>]{3,}?)\s*</i>', re.I)
        pron = p.sub(lambda n: self.__reprgs(n, '', ''), pron)
        p = self.__rex(r'(?<=<i>)\b(sing)\b([\W]?\s*)(</i>)', re.I)
        pron = p.sub(r'\1ular\3\2', pron)
        p = self.__rex(r'<a href="javascript:showEntry\(\'[^<>]*?see[^<>]+?below[^<>]+>(.+?)</a>', re.I)
        pron = p.sub(r'\1', pron)
        if _DEBUG_:
            if self.__rex('<A NAME=', re.I).search(pron):
                    self.__logs.append('E0G:\tcheck pron\t%s\t%s'%(key, pron))
        return ''.join(['<div class="yjs">', pron.rstrip(' ,'), '</div>'])

    def __reppos(self, m):
        text = m.group(1)
        p = self.__rex(r'<[^<>]+>')
        text = p.sub(r'', text)
        return ''.join(['<div><span class="c3r">', text.strip(), '</span></div>'])

    def __cbpos(self, m):
        p = self.__rex(r'<i>\s*([^<>]{3,}?\.)\s*</i>', re.I)
        rg = p.sub(lambda n: self.__reprgs(n, '', ''), m.group(3))
        return ''.join([m.group(1), ', ', rg, m.group(2)])

    def __adjpos(self, m):
        q = m.group(1)
        p = self.__rex(''.join([r'(?<=<q>)\s*(.+?)\(\s*<(em|i)>(', POS, r'\.?)</\2>\s*\)$']))
        return p.sub(r'<span class="khf">\3</span> \1', q)

    def __repeq(self, exmp):
        # xxx = yyy
        p = self.__rex(r'<i>([^<>=]*?[^\d\(\[\s=])\s*=\s*([^\d\)\]\s=][^<>=]*?)</i>', re.I)
        exmp = p.sub(r'<em>\1</em> = <em>\2</em>', exmp)
        p = self.__rex(r'(?<=<em>)([^<>=]*?[^\d\s=])\s*=\s*([^\d\s=][^<>=]*?)(?=</em>)', re.I)
        exmp = p.sub(lambda n: ''.join([self.__rex(r'(\s*[\.;]\s+)').sub(r'</em>\1<em>',
        n.group(1)), '</em> = <em>', n.group(2)]), exmp)
        p = self.__rex(r'<i>([^<>]+)</i>(?=\s*=)', re.I)
        exmp = p.sub(r'<em>\1</em>', exmp)
        return exmp

    def __repexmp(self, exmp):
        exmp = exmp.replace('\xFF\xFF', '')
        n = exmp.count(' ')
        if (n<2 and exmp.find('Figurative')<0) or (n==2 and self.__rex('^<i>[a-z]').search(exmp)) or\
        len(self.__rex('</i>\s*[^<>\(\)\[\],\.\;\?\!\s][^<>\(\)\[\]]+?\s*(?=<i>|$)').findall(exmp)) > 1:
            return ''.join(['<span class="guv">', exmp, '</span>'])
        elif exmp.find('=') > 0:
            exmp = self.__repeq(exmp)
        if self.__rex(r'<i\b[^<>]*>', re.I).search(exmp):
            if exmp.find('[') > -1:
                #</i>...[<i>...</i>]...<i>
                p = self.__rex(r'</i>([^<>\[\]]*\[.+?\][^<>\[\]]*)<i>', re.I)
                q = self.__rex(r'(</?)i(?=>)', re.I)
                exmp = p.sub(lambda n: q.sub(r'\1em', n.group(1)), exmp)
                #</i> [<i>...</i>]
                p = self.__rex(r'(</i>)([^<>\[\]\(\)]*\[.+?\])', re.I)
                exmp = p.sub(lambda n: ''.join([q.sub(r'\1em', n.group(2)), n.group(1)]), exmp)
                #[<i>...</i>] <i>
                p = self.__rex(r'(\[.+?\][^<>\[\]\(\)]*)(<i>)', re.I)
                exmp = p.sub(lambda n: ''.join([n.group(2), q.sub(r'\1em', n.group(1))]), exmp)
            if exmp.find('(') > -1:
                #</i>...(adj.).
                p = self.__rex(''.join([r'(</i>\s*[^<>]*?)\((', POS, r'\.?)\s*\)\s*([\.;]?)']))
                exmp =p.sub(r'(<em>\2</em>)\3\1', exmp)
                #(...)
                p = self.__rex(r'(?<=\()([a-z][^<>]+)(?=\))')
                exmp = p.sub(r'<i>\1</i>', exmp)
                #</i>...(<i>...</i>)...<i>
                p = self.__rex(r'</i>([^<>\(\)]*\([^<>]*<i>.+?</i>\s*\)[^<>\(\)\.]*)<i>', re.I)
                q = self.__rex(r'(</?)i(?=>)', re.I)
                exmp = p.sub(lambda n: q.sub(r'\1em', n.group(1)), exmp)
                #<i>(<i>Fig.</i>)</i> <i>
                p = self.__rex(r'<i>\s*\(\s*<i>\s*(\w+\.?)\s*</i>\s*\)\s*', re.I)
                exmp = p.sub(r'<i>(<em>\1</em>) ', exmp)
                p = self.__rex(r'<i>\s*\(\s*<em>\s*(\w+\.?)\s*</em>\s*\)\s*</i>\s*<i>\s*', re.I)
                exmp = p.sub(r'<i>(<em>\1</em>) ', exmp)
                #</i> (<i>...</i>).#(?:[^<>\(\)]|</?i>)
                p = self.__rex(r'(</i>)([^<>\(\)]*\([^<>]*<i>.+?</i>\)\s*)\.?', re.I)
                exmp = p.sub(lambda n: ''.join([q.sub(r'\1em', n.group(2)), n.group(1)]), exmp)
                #(<i>x+y</i>
                p = self.__rex(r'(?<=\()\s*<i>([^<>]+)</i>', re.I)
                exmp = p.sub(r'<em>\1</em>', exmp)
                #<i>x+y</i>)
                p = self.__rex(r'<i>([^<>]+)</i>\s*(?=\))', re.I)
                exmp = p.sub(r'<em>\1</em>', exmp)
            exmp = exmp.replace('\xFF', '')
            p = self.__rex(r',\s*</i>\s*<i>\s*(?=\w)', re.I)
            exmp = p.sub(r'<span class="rpl"> </span> ', exmp)
            exmp = self.__rex(r',\s*(?=</i>\s*<i>)', re.I).sub('', exmp)
            #</i>...<i>
            p = self.__rex(r'</i>\s*([^<>\(\)\[\],\.\;\?\!\s][^<>\(\)\[\]]+?)\s*<i>', re.I)
            exmp = p.sub(r' <em>\1</em> ', exmp)
            #</i>...
            p = self.__rex(r'</i>\s*([^<>\(\)\[\],\.\;\?\!\s][^<>\(\)\[\]]+?)\s*(?=\(|$)', re.I)
            exmp = p.sub(r' <em>\1</em></i>', exmp)
            #...<i>
            p = self.__rex(r'^\s*([^<>\(\)\[\],\.\;\?\!\s][^<>\(\)\[\]]+?)\s*<i>', re.I)
            exmp = p.sub(r'<i><em>\1</em> ', exmp)
            if self.__rex(r'<i\b[^<>]*>(?:[^<>]|</?[^i/][^<>]*>)*<i\b[^<>]*>', re.I).search(exmp):
                dom = Parser.fromstring(''.join(['<div class="exj">', exmp.strip(), '</div>']))
                for el in dom:
                    if el.tag == 'i':
                        el.tag = 'q'
                exmp = Parser.tostring(dom, encoding='UTF-8')
            else:
                exmp, n = self.__rex(r'(</?)i(?=\b[^<>]*>)', re.I).subn(r'\1q', exmp)
                if n:
                    exmp = ''.join(['<div class="exj">', exmp.strip(), '</div>'])
            if self.__rex(r'<q\b[^<>]*>', re.I).search(exmp):
                # </q> (...).#[^\s]
                p = self.__rex(r'\s*(</q>)\s*([^\w\s\(\[<>]?)\s*(\([A-Z][^<>]+?)\s*(?=<(?:q|/div))')
                exmp = p.sub(r'\2\1 <cite>\3</cite>', exmp)
                p = self.__rex(r'(</q>)(\s*[^\s<>][^<>]*?)\s*(?=<(?:q|cite)\b|$)', re.I)
                exmp = p.sub(r'\2\1', exmp)
                p = self.__rex(r'(?<=<div class="exj">)\s*(.*?)(<q>)', re.I)
                exmp = p.sub(r'\2\1', exmp)
                p = self.__rex(r'\s*(<q>.+?</q>\s*[^\w\s\(\[<>]?(?:\s*<cite>[^<>]+</cite>)?)\s*(?=<q>)', re.I)
                exmp =p.sub(r'<div>\1</div>', exmp)
                p = self.__rex(r'(?<=</div>)\s*(<q>.+?</q>\s*[^\w\s\(\[<>]?(?:\s*<cite>[^<>]+</cite>)?)\s*(?=</div>)', re.I)
                exmp =p.sub(r'<div>\1</div>', exmp)
                p = self.__rex(r'(<q>.+?)\s*(?=[,\.;]?\s*</q>)', re.I)
                exmp = p.sub(self.__adjpos, exmp)
                return exmp
            else:
                return ''.join(['<span class="cv9">', exmp, '</span>'])
        else:
            return ''.join(['<span class="fxe">', exmp, '</span>'])

    def __rep_epc(self, line):
        p = self.__rex(r'(\([^\)]*?|\bExamples?</em>:?[\xFF\s]*)<FONT COLOR="#404040">(.+?)</FONT>', re.I)
        line = p.sub(r'\1<span class="guv">\2</span>', line)
        p = self.__rex(r'<FONT COLOR="#404040">((?:<i>[^<>\s]+</i>\W*?)+)</FONT>', re.I)
        return p.sub(r'<span class="guv">\1</span>', line)

    def __rep_ep(self, line):
        line = self.__rep_epc(line)
        p = self.__rex(r'<FONT COLOR="#404040">(.+?)</FONT>', re.I)
        line = p.sub(lambda m: self.__repexmp(m.group(1)), line)
        p = self.__rex(r'(?<=<span class=")guv(">[^<>]*)<i>([^<>]+)</i>(?=[^<>]*</span>)', re.I)
        return p.sub(r'emj\1\2', line)

    def __rep_ep2(self, line):
        line = self.__rep_epc(line)
        p = self.__rex(r'(:\s*)<FONT COLOR="#404040">(.+?)</FONT>', re.I)
        line = p.sub(lambda m: ''.join([m.group(1), self.__repexmp(m.group(2))]), line)
        p = self.__rex(r'(\w\s*)<FONT COLOR="#404040">(\s*(?:<i>[^<>]+</i>[\s,\.;]*(?:\([^<>]+?\)\.?\s*)?)+)</FONT>(?=\s*\w)', re.I)
        line =p.sub(r'\1<span class="guv">\2</span>', line)
        p = self.__rex(r'<FONT COLOR="#404040">(\s*(?:<i>[^<>]+</i>[\s,\.;]*(?:\([^<>]+?\)\.?\s*)?)+)</FONT>', re.I)
        line =p.sub(lambda m: self.__repexmp(m.group(1)), line)
        p = self.__rex(r'<FONT COLOR="#404040">(.+?)</FONT>', re.I)
        line = p.sub(r'<span class="guv">\1</span>', line)
        return line

    def __rep_ref(self, line):
        p = self.__rex(r'<a href="javascript:showEntry\(\'([^<>]+?)\',\s*\'\s*(ent_[^<>"]+)\'\)"[^<>]*>((?:\s*=\s*)?)((?:[^<>]|</?SUB>)+?(?:&[^<>]{,10};)?)\s*([^\w\s\']{0,2}?)\s*</a>', re.I)
        line =p.sub(r'\3<a href="entry://\1#\2">\4</a>\5', line)
        p = self.__rex(r'<a href="javascript:showEntry\(\'([^<>]+?)\',\s*\'\s*(ent_[^<>"]+)\'\)"[^<>]*>([^<>]*?)\s*<B>\s*(.+?)\s*</B>(.*?)</a>', re.I)
        line =p.sub(r'<span class="gsh">\3 <a href="entry://\1#\2">\4</a>\5</span>', line)
        p = self.__rex(r'(<span class="gsh">)([\xFF\s]+)(?=<a)', re.I)
        return p.sub(r'\2\1', line)

    def __repsyn(self, m):
        syn = m.group(2)
        p = self.__rex(r'^([^<>]+?)(?=\s*(?:<a href=[^<>]+>\s*)?See\b|\s*$)')
        n = p.search(syn)
        if n:
            lns = []
            for ss in n.group(1).strip(' .').split(','):
                ss = ss.strip()
                sk = self.__mk_sk(ss, True)
                if sk.lower() in self.__crefs:
                    lns.append(''.join(['<a href="entry://', sk.replace('/', '%2F'), '">', ss, '</a>']))
                else:
                    lns.append(ss)
            syn = p.sub(''.join([', '.join(lns), '.']), syn) if lns else syn
        syn = self.__rep_ref(syn)
        return ''.join(['<div class="imi"><b>', m.group(1), '</b> ', syn, '</div>'])

    def __chkdef(self, fl, key):
        for f in fl:
            if f.count('(')!=f.count(')') or f.count('[')!=f.count(']'):
                self.__logs.append('E05:\tunbalanced ()/[]\t%s\t%s' % (key, f))
            if len(self.__rex('<i>', re.I).findall(f)) != len(self.__rex('</i>', re.I).findall(f)):
                self.__logs.append('E06:\tunbalanced <i>\t%s\t%s' % (key, f))

    def __b2a(self, m):
        b = m.group(2)
        if b.lower() in self.__crefs:
            return ''.join([m.group(1), '<a href="entry://', b.replace('/', '%2F'), '" class="u8s">', b, '</a>'])
        return m.group(0)

    def __repdef(self, line, clean=False):
        p = self.__rex(r'\s*<A NAME="def_[^<>"]+">\s*</A>\s*', re.I)
        line = p.sub('', line)
        p = self.__rex(r'(?:[\xFF\s]*</?BR/?>|^)[\xFF\s]*((?:(?:\W|\b(?:and|or)\b)*?<i>\s*[^<>]{3,}?\s*</i>)+)', re.I)
        q = self.__rex(r'<i>\s*([^<>]{3,}?)\s*</i>', re.I)
        line =p.sub(lambda m: q.sub(lambda n: self.__reprgs(n, '', ''), m.group(1)), line)
        p = self.__rex(r'<FONT COLOR="#404040"><B>([a-z]{1,2})</B></FONT>\s*', re.I)
        line = p.sub(r'<span class="v9t">\1</span> ', line)
        p = self.__rex(r'<FONT COLOR="#404040"><B>(\([^<>]+)</B></FONT>\s*', re.I)
        line = p.sub(r'<span class="ncd">\1</span> ', line)
        p = self.__rex(r'(<span class="(?:v9t|ncd)">[^<>]+</span>|\bAlso\b,?|</B>\W*|<span class="(?:wb-dict-headword|gpk)">(?:[^<>]|</?B\b[^<>]*>|</?NOBR>)+</span>\W*?|<span class="rg7">[^<>]+</span>\W*?)(?:<BR>|[\xFF\s])*<i>\s*([^<>]{3,}?)\s*</i>', re.I)
        line =p.sub(lambda n: self.__reprgs2(n, ' ', ''), line)
        p = self.__rex(r'\s*<NOBR><SMALL>(\w{3})\w*\(S\):?</SMALL></NOBR>\s*(.+?)[\xFF\s]*(?=<span class="v9t">|$)', re.I)
        line =p.sub(self.__repsyn, line)
        line = self.__rep_ep(line)
        line = self.__rep_ps(line)
        line = self.__rep_ref(line)
        p = self.__rex(r'(<span class="(?:wb-dict-headword|gpk)">)(.+?)(?=</span>)', re.I)
        q = self.__rex(r'(\s*)<B>([a-z][^<>]+)</B>(?!<B\b)', re.I)
        line = p.sub(lambda n: ''.join([n.group(1), q.sub(self.__b2a, n.group(2))]), line)
        p = self.__rex(r'(\w+[^<>]*)<B>([a-z][^<>]+)</B>(?!<B\b)', re.I)
        line = p.sub(self.__b2a, line)
        p = self.__rex(r'(_{3,})')
        line = p.sub(r'<span class="ivc">\1</span>', line)
        cls = 'ljb' if self.__rex(r'^[\xFF\s]*<span class="v9t">').search(line) else 'bi0'
        p = self.__rex(r'(<span class="v9t">.+?)(?=<span class="v9t">|$)', re.I)
        line = p.sub(''.join(['<div class="', cls, r'">\1</div>']), line)
        if clean:
            line = self.cleansp(line).strip()
        return line

    def __repdef2(self, line, gp1=''):
        if line.strip():
            p = self.__rex(r'^\s*([\(\[]?<B>(?:</?B\b[^<>]*>|[^<>])+</B>[\)\]]*)([,\.]?)\s*(?:<BR>)?', re.I)
            line = p.sub(r'<span class="gpk">\1</span>\2 ', line)
            line = self.__rex(r'^(?:\s|</?BR/?>)+|(?:\s|</?BR/?>)+$', re.I).sub('', line)
            line = ''.join(['<div class="ihl">', self.__repdef(line, True), '</div>'])
            p = self.__rex(r'(?<=</B>)\s*\|\s*(<B)(?=>)', re.I)
            line = p.sub(r'\1 class="ykc"', line)
            p = self.__rex(r'(<B>.+?</B>)', re.I)
            line = p.sub(lambda m: self.__rex(r'</?NOBR>', re.I).sub('', m.group(1)), line)
            p = self.__rex(r'(<span class="(?:wb-dict-headword|gpk)">(?:[^<>]|</?B\b[^<>]*>)+</span>)\s*<BR>(?:<BR>|\s)*(?=\w)', re.I)
            line =p.sub(r'\1, ', line)
            p = self.__rex(r'((?:<span class="(?:wb-dict-headword|rg7|gpk)">(?:[^<>]|</?B\b[^<>]*>)+</span>|<(em|b)>[\w]+\.?</\2>)\W*?)<BR>', re.I)
            line =p.sub(r'\1 ', line)
            line = self.__rex(r'(=\s*<a href=[^<>]+>[^<>]+</a>)\s*(?=\w)', re.I).sub(r'\1. ', line)
        return ''.join([gp1, line])

    def __repinfl2(self, line, key):
        line = self.__rex(r'^(?:\s|</?BR/?>)+|(?:\s|</?BR/?>)+$', re.I).sub('', line)
        p = self.__rex(r'</?(?:div|q|fieldset)\b', re.I)
        if line and not p.search(line):
            if _DEBUG_:
                if self.__rex('<A NAME=', re.I).search(line):
                    self.__logs.append('E0I:\tcheck <a>\t%s\t%s'%(key, line))
            n = 0
            if line.find('<em>plural') > -1:
                p = self.__rex(r'^((?:<em>plural</em>|[^<>]|</?(?:[Bi]|NOBR|span[^<>]*)>)+)$', re.I)
                line, n = p.subn(lambda n: self.__repinfl(n, key), line)
            if not n:
                line = ''.join(['<div class="qjx">', self.__repdef(line, True), '</div>'])
        return line

    def __repdef3(self, line, key=''):
        p = self.__rex(r'</?(?:div|q|fieldset)\b', re.I)
        if line and not p.search(line):
            return self.__repdef2(line)
        else:
            return line

    def __replast(self, line, p, func, key=''):
        ops, pos, pl = 0, 0, []
        for m in p.finditer(line):
            pos = m.start()
            part = line[ops:pos]
            idx = part.rfind('</div>')
            if idx>0 and pos-idx>6:
                pl.append(''.join([part[:idx+6], func(part[idx+6:], key)]))
            else:
                pl.append(part)
            ops = pos
        if pos:
            pl.append(line[pos:])
            line = ''.join(pl)
        return line

    def __mk_sk(self, hd, u2a=False):
        sk = self.__rex(r'&rsquo;|&#146;', re.I).sub('\'', self.__rex(r'&acute;|\xFF\xFF', re.I).sub('', self.__rep_hd(hd)))
        if sk.find('&') > -1:
            if u2a:
                sk = sk.replace('&amp;', '&"')
                p = self.__rex(r'&([a-z])\w+;', re.I)
                sk = p.sub(r'\1', sk).replace('&"', '&')
                p = self.__rex(r'&#x\w{4};')
                sk = p.sub(lambda m: self.__uc_tbl[m.group(0)] if m.group(0) in self.__uc_tbl else m.group(0), sk)
            else:
                dom = Parser.fromstring(sk)
                sk = self.__ref2a(Parser.tostring(dom, encoding='UTF-8')).replace('&amp;', '&')
        return self.__rex(r'</?[^<>]+>').sub('', sk).strip()

    def __repdrv(self, m):
        hd, pos = self.__rep_hd(self.__rm_cm(m.group(1))), m.group(2)
        if self.__rex(r'<FONT color="#404040">', re.I).search(pos):
            pos = self.__rep_ep(pos)
        if pos.strip(' \xFF.'):
            pos = self.__rep_ps(pos).lstrip(' ,')
            pos = ''.join([', ', pos])
        return ''.join(['<div class="duy">', hd, pos, '</div>'])

    def __rm_cm(self, hd):
        p = self.__rex(r'(<span class="wb-dict-headword">.+?)[\,\s]*(</B>\s*</span>[^<>]*)$', re.I)
        return p.sub(r'\1\2', hd)

    def __fix_tt(self, hd):
        p = self.__rex(r'(?<=\()([^\(\)]+?)(?=\))', re.I)
        q = self.__rex(r'<span class="wb-dict-headword">((?:</?B[^<>]*>|[^<>])+)</span>', re.I)
        hd = p.sub(lambda m: q.sub(r'\1', m.group(1)), hd)
        p = self.__rex(r'</span>(\s*\([^\(\)]+?</B>[^\(\)]*\)\s*)<span class="wb-dict-headword">', re.I)
        hd = p.sub(r'\1', hd)
        p = self.__rex(r'(</span>)(\s*\([^\(\)]+?</B>[^\(\)]*\)\s*)$', re.I)
        hd = p.sub(r'\2\1', hd)
        return hd

    def __fmt_hd(self, hd):
        if _DEBUG_:
            p = self.__rex(r'</B></span>[\xFF\s]*<span class="wb-dict-headword">', re.I)
            if p.search(hd):
                self.__logs.append('W08:\tcheck title\t%s'%hd)
        hd = self.__rm_cm(hd)
        p = self.__rex(r'(?<=<B>)([^<>]+)(?=</B>)', re.I)
        q = self.__rex(r'(\s*[\(\)]\s*)')
        hd = p.sub(lambda n: q.sub(r'</B>\1<B>', n.group(1)), hd)
        return re.compile(r'<B>(\s*)</B>', re.I).sub(r'\1', hd)

    def __fmt_pd(self, hd, tx):
        return ''.join(['<div class="xmk">', self.__fmt_hd(hd), '</div><div class="j5c">', self.__repdef(tx), '</div>'])

    def __repphvdrv(self, m):
        text = m.group(2)
        pos = text.find('\xFF\xFF')
        assert pos > 40
        hd, tx = text[:pos], text[pos:]
        p = self.__rex(r'^([\(\[]?<span class="wb-dict-headword">\s*(?:[^\xFF\s<>]|</?B\b[^<>]*>)+?\s*</span>[\]\)]?)[^<>\w]*$', re.I)
        q = self.__rex(r'<BR/?>', re.I)
        n = p.search(hd)
        if n:
            ent = ''.join(['<div class="dlq">', self.__fmt_pd(n.group(1), q.sub('', tx)), '@</div>'])
        else:
            ent = ''.join(['<div class="a4p">', self.__fmt_pd(hd.rstrip(' \\,.'), q.sub('', tx)), '#</div>'])
        return ''.join([m.group(1), ent])

    def __regphv(self, m, key, phvs):
        hd, tx = m.group(1), m.group(2)
        sk = self.__mk_sk(hd)
        phvs.append((sk, ''.join(['<link rel="stylesheet"href="', self.DIC_T,
        '.css"type="text/css"><div class="wcv">', hd, tx,
        '<span class="thw">See parent entry: <a href="entry://',
        key.replace('/', '%2F'), '">', key, '</a></span></div>'])))
        return ''.join(['<p><a href="entry://', sk.replace('/', '%2F'), '">',
        self.__rex(r'</?[^<>]+>').sub('', hd), '</a></p>'])

    def __adjs_i(self, line):
        p = self.__rex(r'(<(i|em)[^<>]*>)((?:[^<>]|</?(?:NOBR|SMALL)>)+)(?=</\2>)', re.I)
        q = self.__rex(r'(\s*(?:[\(\)\[\],\.=\+:]|&lt;)\s*)')
        line = p.sub(lambda m: ''.join([m.group(1), q.sub(''.join([r'</', m.group(2), r'>\1<', m.group(2), r'>']), m.group(3))]), line)
        line = self.__rex(r'<(i|em)>(\s*)</\1>', re.I).sub(r'\2', line)
        p = self.__rex(r'<(i|em)>(\s*\(<(i|em)>[^<>]+</\3>\)\s*)</\1>', re.I)
        return p.sub(r'\2', line)

    def __repety(self, m, key):
        ety = m.group(1)
        if _DEBUG_:
            if ety.count('[') != ety.count(']'):
                self.__logs.append('E07:\tunbalanced []\t%s\t%s' % (key, ety))
        ety = self.__adjs_i(self.__rep_ref(ety))
        p = self.__rex(r'<nobr>(.+?)(\]?\s*)</nobr>', re.I)
        ety = p.sub(r'<span class="wje">\1</span>\2', ety)
        p = self.__rex(r'(?<=<i>)([^<>]+)(?=</i>)', re.I)
        q = self.__rex(r'(\s*(?:&lt;|[\(\)])\s*)')
        ety = p.sub(lambda n: q.sub(r'</i>\1<i>', n.group(1)), ety).replace('<i></i>', '')
        p = self.__rex(r'(?<=\])\.?[\xFF\s]*$', re.I)
        ety, n = p.subn('</div>', ''.join(['<div class="epl">', ety]))
        if n != 1:
            self.__logs.append('E08:\tcheck ety\t%s\t%s'%(key, ety))
        return ety

    def __repnote(self, m, cls, et):
        nt = m.group(1)
        p = self.__rex(r'(?:\s|<BR>)*<NOBR><I><B>&ndash;\s*<i>(.+?)</B></I></NOBR>', re.I)
        nt = p.sub(lambda n: self.__rep_pt(self.__rex(r'<[^<>]+>').sub('', n.group(1))), nt)
        p = self.__rex(r'<NOBR><I><B>\s*&ndash;\s*([^<>]+?)\s*\.?\s*</B></I></NOBR>(?:<BR>|[\xFF\s])*', re.I)
        nt = p.sub(r'<legend>\1</legend>', nt)
        p = self.__rex(r'(<i>[^<>]+?)(:\s*)(</i>)', re.I)
        nt = p.sub(r'\1\3\2', nt)
        nt = self.__rep_ep2(nt)
        nt = self.__rep_ref(nt)
        nt = self.__rep_ps(nt)
        p = self.__rex(r'(?<=<span class=")wb-dict-headword"><B>\s*(\d+)\s*</B>(</span>)', re.I)
        nt = p.sub(r'xbg">\1\2 ', nt)
        p = self.__rex(r'(?<=<span class=")wb-dict-headword"><B>\s*([a-z]{1,2})\s*</B>(</span>)', re.I)
        nt = p.sub(r'ujn">\1\2 ', nt)
        p = self.__rex(r'(<span class="xbg">.+?)(?=<span class="xbg">|$)', re.I)
        nt = p.sub(r'<div class="lix">\1</div>', nt)
        p = self.__rex(r'<B>\s*(\d+)\s*</B>\s*', re.I)
        nt = p.sub(r'<span class="qhq">\1</span> ', nt)
        p = self.__rex(r'((?<!</B>))<B>([a-z][^<>]+)</B>(?!<B\b)', re.I)
        nt = p.sub(self.__b2a, nt)
        return ''.join(['<fieldset class="', cls, '">', nt, et, '</fieldset>'])

    def __trans_pos(self, m):
        pos = m.group(1)
        if pos in self.__pos_tbl:
            return self.__pos_tbl[pos]
        else:
            p = self.__rex(r'\bn\.[,;\s]*(?=\w)')
            pos = p.sub('noun, ', m.group(0))
            p = self.__rex(r'\bn\.\s*$')
            pos = p.sub('noun', pos)
            p = self.__rex(r'\bv\.[,;\s]*(?=[^\Wit])')
            pos = p.sub('verb, ', pos)
            p = self.__rex(r'\bv\.\s*$')
            pos = p.sub('verb', pos)
            pos = self.__rex(r'([,;]+)').sub(r'<span>\1</span>', pos)
            pos = self.__rex(r'\b(and|or)\b').sub(r'<em>\1</em>', pos)
            return pos

    def __fix_var(self, m):
        rg, var = m.group(1), m.group(2)
        p = self.__rex(r'(Also,?)[\xFF\s]*<i>\s*([^<>]{3,}?)\s*</i>', re.I)
        rg = p.sub(lambda n: self.__reprgs2(n, ' ', ''), rg)
        return ''.join([m.group(3), '<div class="duv">', rg, self.__rep_hd(var), '</div>'])

    def __fix_pos(self, key, line):
        i = line.find('<span class="c3r">')
        if i > 0:
            p = self.__rex(r'<div class="(?:ihl|bvn)">', re.I)
            m = p.search(line)
            if m.start() < i:
                p = self.__rex(r',?\s*<span class="kce">([^<>,]+</span>)[\.,]?(?=</div><div class="(?:ihl|bvn)">)', re.I)
                line, n = p.subn(r'</div><div><span class="c3r">\1', line, 1)
                if n < 1:
                    p = self.__rex(r'(?<=</div><div class=")ihl(">[^<>]+?)(?=</div><div>$)', re.I)
                    lp, n = p.subn(r'qjx\1', line[:i], 1)
                    if n:
                        line = ''.join([lp, line[i:]])
                    else:
                        self.__logs.append('W07:\tmiss pos-tag\t%s' % key)
        return line

    def __fix_line(self, m):
        line = m.group(1)
        line = re.compile(r'</?(?:ol|li)>', re.I).sub('', line)
        line = line.replace('<B>c</B>', '<B>d</B>').replace('<B>b</B>', '<B>c</B>').replace('<B>a</B>', '<B>b</B>')
        p = re.compile(r'(<A NAME="def_\d+__1"></A><i>Rugby)', re.I)
        line = p.sub(r'<FONT COLOR="#404040"><B>(2)</B></FONT> \1', line)
        return ''.join(['<FONT COLOR="#404040"><B>a</B></FONT> <FONT COLOR="#404040"><B>(1)</B></FONT> ', line])

    def __spec_fix(self, key, line):
        if key == 'cheese cake':
            line = line.replace('<i>Slang.</i>', '<span class="rg7">Slang.</span>')
        elif key == 'complementarity':
            line = line.replace('<i>Physics.</i>', '<span class="rg7">Physics.</span>')
        elif key == 'slenium':
            key = 'selenium'
            line = line.replace('sle', 'sele')
        elif key == 'rigveda':
            key = 'rig-veda'
            line = line.replace('Rigve', 'Rig-ve')
        elif key == 'vinculum':
            line = line.replace('a+b', '<span class="xhe">a+b</span>')
        elif key == 'line':
            p = re.compile(r'<ol>[\xFF\s]*<li>(<A NAME="def_\d+__"></A><i>Baseball.+?)</ol>', re.I)
            line = p.sub(self.__fix_line, line)
        elif key == 'mohs':
            key = 'mohs scale'
        elif key == 'schrodinger':
            key = 'schrodinger equation'
        elif key == 'play on':
            key = 'play on words'
        elif key == 'secondary sex':
            key = 'secondary sex characteristic'
        return key, line

    def split_entry(self, key, line):
        if line == '###':
            return ''
        else:
            p = self.__rex(r'(<span class="wb-dict-headword">.+?</span>)\s*(<A NAME="ent_[^<>"]+">\s*</A>)', re.I)
            line = p.sub(r'\2\1', line)
            p = self.__rex(r'(<A NAME="ent_[^<>"]+">\s*</A>.+?)\s*(?=<A NAME="ent_[^<>"]+">\s*</A>|$)', re.I)
            lns = []
            for entry in p.findall(line):
                q = self.__rex(r'<span class="wb-dict-headword">\s*(.+?)\s*</span>', re.I)
                key = self.__mk_sk(q.search(entry).group(1))
                if key in self.__words_need_extr:
                    lns.append(self.extr_entry(key, entry))
                else:
                    lns.append(self.format_entry(key, entry))
            return '\n'.join(lns)

    def __mk_wlist(self, lns, key, line, sfx=''):
        ref = ''.join(['word-list-', key, sfx])
        p = self.__rex(r'<A NAME="pfx_[^<>]+"></A><FONT class="dictionary" COLOR="#006600">(.+?)</FONT>', re.I)
        lns.append('\n'.join([ref, ''.join(['<link rel="stylesheet"href="WL.css"type="text/css"><div class="fle">',
        '<div>Back to <a href="entry://', key, '">', key, '</a></div>',
        ''.join([''.join(['<p class="a4p">', self.__tag2lower(self.__rep_hd(self.__correct_data(key, w))), '</p> ']) for w in p.findall(line)]),
        '<div>Back to <a href="entry://', key, '">', key, '</a></div>',
        '</div>']), '</>']))
        if _DEBUG_:
            print "%s generated" % ref
        return ''.join([' <span class="pqn"><a href="entry://', ref, '">',
        ref, '</a></span>'])

    def extr_entry(self, key, line):
        p = self.__rex(r'(?:[\xFF\s]|<BR>)*(<A NAME="pfx_[^<>]+"></A>.+?)(?=</?(?:ol|li|p|BR)\b)', re.I)
        lns, count, wl = [], 1, p.findall(line)
        if len(wl)>1:
            for wb in wl:
                line = p.sub(self.__mk_wlist(lns, key, wb, ''.join(['-', str(count)])), line, 1)
                count += 1
        else:
            line = p.sub(self.__mk_wlist(lns, key, line), line)
        line = self.format_entry(key, line)
        p = self.__rex(r'(?:\s|<BR>)*<FONT class="dictionary">(.+?)</FONT>', re.I)
        lns.append(p.sub(r' <span class="vfv">\1</span>', line))
        return '\n'.join(lns)

    def format_entry(self, key, line, make_entry=True):
        if line == '###':
            return '\n'.join([key, line, '</>'])
        key, line = self.__spec_fix(key, line)
        line = self.__correct_data(key, line)
        p = self.__rex(r'(<span class="wb-dict-headword">[^\xFF]+?</span>)\s*(<A NAME="ent_[^<>"]+">\s*</A>)', re.I)
        line =p.sub(r'\2\1', line)
        p = self.__rex(r'<A NAME="ent_[^<>"]+">\s*</A>', re.I)
        m = p.search(line)
        if m:
            line = line[m.start():]
        else:
            raise AssertionError('"%s" may be empty' % key)
        p = self.__rex(r'<hr[^<>]*>', re.I)
        hrs = p.findall(line)
        if hrs:
            lasthr = hrs[-1]
            pos = line.rfind(lasthr)
            line, rmd = line[:pos], line[pos:]
            n = 1
            while n:
                p = self.__rex(r'</?[^<>]+>|[\xFF\s]+')
                rmd, n = p.subn('', rmd)
            assert not rmd
        else:
            self.__logs.append('I03:\tThere is no HR in %s' % key)
        if _DEBUG_:
            p = self.__rex(r'<A NAME="ent_[^<>"]+">\s*</A><span class="wb-dict-headword"><B>([^<>]+)</B></span> or <span class="wb-dict-headword"><B>\1')
            if p.search(line):
                self.__logs.append('W09:\tcheck title\t%s'%key)
            p = self.__rex(r'<FONT COLOR="#404040"><B>a</B></FONT>', re.I)
            q = self.__rex(r'<FONT COLOR="#404040"><B>b</B></FONT>', re.I)
            if len(p.findall(line)) != len(q.findall(line)):
                self.__logs.append('E0J:\tcheck index-abc\t%s' % key)
            p = self.__rex(r'<FONT COLOR="#404040"><B>[a-z]{1,2}</B></FONT>[\xFF\s]*<FONT COLOR="#404040"><i>', re.I)
            if p.search(line):
                self.__logs.append('E0O:\tcheck abc-exm\t%s' % key)
        p = self.__rex(r'(?<=<span class="wb-dict-headword">)\s*([^\xFF]+?)\s*(?=</span>)', re.I)
        line = p.sub(lambda m: self.__rep_hd(m.group(1)), line)
        p = self.__rex(r'((?:^|\xFF\xFF)\s*<A NAME="ent_[^<>"]+">\s*</A>)\s*(<span class="wb-dict-headword">.+?)\s*\,?(?=[\xFF\s]*(?:<(?:/?BR|i|em|ol|A)\b|&laquo;))', re.I)
        if _DEBUG_:
            q = self.__rex(r'</B></span>[\(\)\s]*<span class="wb-dict-headword"><B>', re.I)
            for g1, g2 in p.findall(line):
                if q.search(g2):
                    self.__logs.append('W09:\tcheck title\t%s\t%s' % (key, g2))
        line, n =p.subn(lambda m: ''.join(['<div class="nek">', m.group(1), self.__fmt_hd(self.__fix_tt(m.group(2))), '</div>']), line)
        if not n:
            raise AssertionError('%s: no title'%key)
        p = self.__rex(r'(&laquo;.+?)(?=</?(?:BR|ol|p|li|div)\b)', re.I)
        line = p.sub(lambda m: self.__rep_pr(m.group(1)), line)
        if _DEBUG_:
            p = self.__rex(r'(?:</?(?:BR|ol|p|li|div)[^<>]*>|\xFF\xFF)[\xFF\s]*(&laquo;.+?)(?=</?(?:BR|ol|p|li|div)\b)', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('E03:\tcheck\t%s\t%s' % (key, m.group(1)))
            p = self.__rex(r'(?:&raquo;|</?(?:BR|ol|p|li|div)[^<>]*>|<span class="wb-dict-headword">.+?</span>)([^&]+)(?=&raquo;)', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('E04:\tcheck\t%s\t%s&#187;' % (key, m.group(1)))
        p = self.__rex(r'<i>((?:no )?\s*pl)(?:ural)?([\W]?\s*)</i>', re.I)
        line = p.sub(r'<em>\1ural</em>\2', line)
        p = self.__rex(r'<i>(\s*(?:Abbr|Symbol|Examples?|Formula))([\W]?\s*)</i>')
        line = p.sub(r'<em class="y2c">\1</em>\2', line)
        p = self.__rex(r'((?:[\xFF\s]|<BR>)*)(<i>genitive</i>[^<>]*?)((?:[\xFF\s]|<BR>)*)(<span class="wb-dict-headword">.+?</span>[^\w<>]*)', re.I)
        line = p.sub(r' \2 \4\1\3', line)
        p = self.__rex(r'(</(?:i|em|B)>\.?)[\xFF\s]*<BR>(<i>[^<>]+</i>)(?=(?:[\xFF\s]|<BR>)*<NOBR>)', re.I)
        line = p.sub(r'\1 \2', line)
        p = self.__rex(r'(?<=</div>)[\xFF\s]*(.*?),?\s*(?=<(?:/?BR|ol|p)\b|[^<>]*<A\b)', re.I)
        line = p.sub(lambda m: self.__repinfl(m, key), line)
        line, n = self.__rex(r'(?<=<li>)(?:[\xFF\s]|<A NAME="def_[^<>]+"></A>)*(?=Also\b)', re.I).subn('', line)
        p = self.__rex(r'(?<!<li>)(Also\b,?[\xFF\s]*(?:<i>[^<>]+</i>[\xFF\s]*)?)((?:[^<>]{0,10}?(?:before vowels.\s*Also,\s*)?<span class="wb-dict-headword">(?:</?B\b[^<>]*>|</?NOBR>|[^<>])+</span>)+\s*(?:before vowels|\(for\b[^<>\(\)]+\))?\s*[^<>\w]*)(</li></ol>)', re.I)
        line =p.sub(self.__fix_var, line)
        if _DEBUG_:
            p = self.__rex(r'<span class="wb-dict-headword">(?:</?B\b[^<>]*>|</?NOBR>|[^<>])+</span>[^<>]{0,20}</li></ol>', re.I)
            for f in p.findall(line):
                self.__logs.append('W03:\tcheck var\t%s\t%s'%(key, f))
            p = self.__rex(r'<ol>(.+?)</ol>', re.I)
            q = self.__rex(r'<NOBR>\s*&ndash;', re.I)
            for f in p.findall(line):
                if q.search(f):
                    self.__logs.append('E0K:\tcheck drv\t%s'%key)
        p = self.__rex(r'(?:[\xFF\s]|<BR>)*<NOBR>\s*&ndash;\s*(<span class="wb-dict-headword">(?:[^<>]|</?(?:B|em)\b[^<>]*>)+</span>)\s*</NOBR>(.*?)\s*(?=</?(?:BR|ol|p|table)\b)', re.I)
        line =p.sub(self.__repdrv, line)
        p = self.__rex(r'\s*<A NAME="use_[^<>"]+">\s*</A>\s*(.+?)(?=<BR>\s+<BR>|<BR><A NAME|</?(?:fieldset|ol|p)\b)', re.I)
        line, fn1 =p.subn(lambda m: self.__repnote(m, 'fvo', '@'), line)
        p = self.__rex(r'\s*(<NOBR><I><B>\s*&ndash;\s*Synonym.+?)(?=<BR>\s+<BR>|<BR><A NAME|</?(?:fieldset|ol|p)\b)', re.I)
        line, fn2 =p.subn(lambda m: self.__repnote(m, 'sgy', '#'), line)
        p = self.__rex(r'(?<=\xFF\xFF)[\xFF\s]*<NOBR><I><B>&ndash;([^\xFF]+?)</B></I></NOBR>', re.I)
        line = p.sub(self.__reppos, line)
        p = self.__rex(r'(<div><span class="c3r">[^<>]+</span>)(</div>)[,\.]?[\xFF\s]*((?:\W*?<(i|em)>[^<>]+</\4>)+)[\xFF\s]*(?=</?(?:BR|ol|p|div)\b)', re.I)
        line =p.sub(self.__cbpos, line)
        if _DEBUG_:
            p = self.__rex(r'\s*<FONT COLOR="#404040">\s*(.+?)\s*</FONT>\s*', re.I)
            self.__chkdef(p.findall(line), key)
        p = self.__rex(r'(<BR>\s*<BR>)\s*([^<>\xFF]*<span class="wb-dict-headword">[^\xFF]+?</span>.+?)(?=<BR>\s*<BR>|</?p\b|<div|<fieldset)', re.I)
        line =p.sub(self.__repphvdrv, line)
        p = self.__rex(r'(?<=<li>)\s*(.+?)\s*(?=</li>)', re.I)
        line = p.sub(lambda m: self.__repdef(m.group(1)), line)
        p = self.__rex(r'\s*<a name="ety_[^<>"]+">\s*</a>(.+?)(?=</?(?:BR|ol|p|div|fieldset)\b)', re.I)
        line = p.sub(lambda m: self.__repety(m, key), line)
        if _DEBUG_:
            p = self.__rex(r'<li>(.+?)</li>', re.I)
            for f in p.findall(line):
                if self.__rex('<p>|<a name="ety\b', re.I).search(f):
                    self.__logs.append('E08:\tcheck ety\t%s\t%s'%(key, f))
            m = self.__rex(r'<a name="ety_[^<>"]+">.+?(?=</?(?:BR|ol|p)\b)', re.I).search(line)
            if m:
                self.__logs.append('E09:\t%s: ety not formatted %s' % (key, m.group(0)))
            if self.__rex('&[lr]aquo\b', re.I).search(line):
                self.__logs.append('E0A:\t%s: pron not formatted'%key)
        p = self.__rex(r'<TABLE BORDER="0" CELLSPACING="0" BGCOLOR="#E7E7E7">', re.I)
        line, n = p.subn(r'<table border=0 cellspacing=0 class="g95">', line)
        if n:
            p = self.__rex(r'<FONT class="dictionary" SIZE="-1">([=\s]*)</FONT>', re.I)
            line = p.sub(r'\1', line)
            p = self.__rex(r'(?<=<FONT class="dictionary" SIZE="-1">)([^<>]+?)(=)\s*(</FONT></TD>)', re.I)
            line = p.sub(r'\1\3<TD ALIGN="CENTER">\2</TD>', line)
            p = self.__rex(r'(<TD\b[^<>]*>\s*<FONT class="dictionary" SIZE="-1">)\s*(=)([^<>]+)(?=</FONT></TD>)', re.I)
            line = p.sub(r'<TD ALIGN="CENTER">\2</TD>\1\3', line)
            p = self.__rex(r'<FONT class="dictionary" SIZE="-1">(.+?)</FONT>', re.I)
            line = p.sub(r'<span class="r6h">\1</span>', line)
            p = self.__rex(r'<TR VALIGN="TOP" BGCOLOR="#E7E7E7">', re.I)
            line = p.sub(r'<tr class="fph">', line)
        p = self.__rex(r'(<TD\b[^<>]*>)<FONT class="dictionary">(.*?)</FONT>\s*(?=</TD>)', re.I)
        line = p.sub(r'\1<span class="keq">\2</span>', line)
        p = self.__rex(r'(?<=<TD )\s*NOWRAP(?=>)', re.I)
        line = p.sub(r'class="cov"', line)
        p = self.__rex(r'<hr[^<>]*>', re.I)
        line = p.sub(r'</div><div class="btx">', ''.join(['<div class="btx">', line, '</div>']))
        line = self.__rex(r'[\xFF\s]+').sub(' ', line)
        line = self.cleansp(line)
        if fn1 or fn2:
            p = self.__rex(r'@</fieldset><fieldset class="fvo">(.+?)@(?=</fieldset>)', re.I)
            line = p.sub(r'<div class="ovs">\1</div>', line)
            p = self.__rex(r'#</fieldset><fieldset class="sgy">(.+?)#(?=</fieldset>)', re.I)
            line = p.sub(r'<div class="ovs">\1</div>', line)
            p = self.__rex(r'\s*[@#](?=</fieldset>)', re.I)
            line = p.sub(r'', line)
        if _DEBUG_:
            m = self.__rex(r'.{0,2}<[^>]*?<[^<>]+>|<[^<>]+>[^<]*?>.{0,2}').search(line)
            if m:
                self.__logs.append('E0B:\tunbalanced <>\t%s\t%s' % (key, m.group(0)))
            p = self.__rex(r'</ol>\s*[^<>]+', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('W05:\tcheck ol\t%s\t%s'%(key, m.group(0)))
        p = self.__rex(r'(<div><span class="c3r">[^<>]+</span></div>)(.*?)(?=</?(?:ol|p|div|fieldset)\b)', re.I)
        line =p.sub(lambda m: self.__repdef2(m.group(2), m.group(1)), line)
        line = self.__tag2lower(line)
        p = self.__rex(r'</?p>', re.I)
        line = p.sub('', line)
        p = self.__rex(''.join([r'(<div class="nek">.+?</div>)<i>(', POS, r'[^<>]*?)</i>(?=<ol>)']))
        line =p.sub(r'\1<div><span class="c3r">\2</span></div>', line)
        if _DEBUG_:
            p = self.__rex(r'<div class="yjs">(.+?)</div>', re.I)
            for f in p.findall(line):
                if self.__rex('<A\b', re.I).search(f):
                    self.__logs.append('E0H:\tcheck anchor\t%s\t%s'%(key, f))
            p = self.__rex(r'</ol>(.+?)(?=<ol>|<div><span class="c3r">)', re.I)
            for f in p.findall(line):
                if f.find('</div>') < 0:
                    self.__logs.append('E0C:\tcheck pos\t%s\t%s'%(key, f))
            p = self.__rex(r'<ol>(.+?)</ol>', re.I)
            for f in p.findall(line):
                if f.count('<li>') == 1:
                    self.__logs.append('W04:\tcheck ol\t%s\t%s'%(key, f))
        line = self.__replast(line, self.__rex(r'<ol>', re.I), self.__repinfl2, key)
        line = line.replace('<ol>', '<ol class="bvn">').replace('<li>', '<li class="l1v">')
        p = self.__rex(r'(</?)(?:ol|li)\b', re.I)
        line = p.sub(r'\1div', line)
        pos = line[:-6].rfind('</div>')
        line = ''.join([line[:pos+6], self.__repdef3(line[pos+6:-6]), '</div>'])
        p = self.__rex(r'<div class="(?:epl|duy|duv|dlq|a4p)">|<fieldset [^<>]+>|<div><span class="c3r">|(?:</div>)?<div class="btx">', re.I)#ab origine
        line = self.__replast(line, p, self.__repdef3)
        p = self.__rex(r'(?<=<div class="btx">)(.+?)(?=<div class="btx">|$)', re.I)
        line = p.sub(lambda m: self.__fix_pos(key, m.group(1)), line)
        line = line.replace('<span class="wb-dict-headword">', '<span class="upo">')
        p = self.__rex(r'(&radic;|&#x221[AB];)((?:[^<>\s,\+\-\*/]|\s*[\+\-/\*]\s*)+)', re.I)
        line = p.sub(r'<span class="cyp">\1<span class="xhe">\2</span></span>', line)
        p = self.__rex(r'(?<=<span class="c3r">)\s*([^<>]+?)[,;\.\s]*(?=</span>)', re.I)
        line = p.sub(self.__trans_pos, line)
        p = self.__rex(r'(?<=<span class="rg7">)([^<>]+)(?=</span>)', re.I)
        line = p.sub(lambda m: self.__rex(r'([\(\)])').sub(r'<span>\1</span>', m.group(1)), line)
        p = self.__rex(r'(?<=#</div><div class=")dlq(">.+?)@(?=</div>)', re.I)
        line = p.sub(r'a4p\1#', line)
        line = self.__adjs_i(line)
        p = self.__rex(r'(?<=>)\.(?=<(?:i|em)>(?:plural|singular))', re.I)
        line = p.sub(r', ', line)
        p = self.__rex(r'(<(i|em)>(?:plural|singular)</\2>)\.\s*(?=<(?:B|em|i)\b)', re.I)
        line = p.sub(r'\1 ', line)
        p = self.__rex(r'<NOBR>(.+?)</NOBR>', re.I)
        line = p.sub(r'<span class="cyp">\1</span>', line)
        p = self.__rex(r'<SMALL>([^<>]+)</SMALL>', re.I)
        line = p.sub(r'<span class="ah7">\1</span>', line)
        p = self.__rex(r'<img [^<>]*?src="p/([^<>]+?)"[^<>]*>', re.I)
        line = p.sub(lambda m: self.__chrimg_list[m.group(1)] if m.group(1) in self.__chrimg_list else m.group(0), line)
        assert not self.__rex(r'<img [^<>]*?src="p/([^<>]+?)"[^<>]*>', re.I).search(line)
        if _DEBUG_:
            p = self.__rex(r'<div class="(?:nek|yjs|epl|duy)">.+?</div>', re.I)
            nline = p.sub(r'', line)
            p = self.__rex(r'.{10}[\s>][b-z]\s.{10}')
            for f in p.findall(nline):
                self.__logs.append('W0A:\tcheck spelling\t%s\t%s' % (key, f))
            p = self.__rex(r'<div class="\w+">\W*</div>', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('E0M:\tcheck div\t%s\t%s'%(key, m.group(0)))
            p = self.__rex(r'<span class="cyp">\s*&ndash;', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('E0N:\tcheck drv\t%s\t%s'%(key, m.group(0)))
            p = self.__rex(r'<div class="ihl"><span class="kce">[^<>]+</span></div>', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('W06:\tcheck drv-pos\t%s\t%s'%(key, m.group(0)))
            p = self.__rex(r'[@#]</div><div class="ihl">', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('E0E:\tphv not formated\t%s\t%s'%(key, m.group(0)))
            p = self.__rex(r'(<a href="[^<>]+?").+?</a>', re.I)
            for a in p.finditer(line):
                ref = a.group(1)
                if ref.find('&')>-1 or ref.find('javascript:')>0:
                    self.__logs.append('E0D:\tcheck x-ref\t%s\t%s' % (key, a.group(0)))
            p = self.__rex(r'(?<=<span class="(?:rg7)">)\s*([^<>]+?)\s*(?=\.|<)')
            for l in p.findall(line):
                if l in self.lang_d:
                    if len(self.lang_d[l]) > 20:
                        self.lang_d[l] = ['*']
                    elif self.lang_d[l][0] != '*':
                        self.lang_d[l].append(key)
                else:
                    self.lang_d[l] = [key]
            p = self.__rex(r'(?<=<i>)\s*((?:(?:especially|chiefly|in(?: the)?|now)\s+)?[A-Z][\w\s\.]{2,}?[\.\)])\s*(?=</i>)')
            for l in p.findall(line):
                if l in self.lang_d2:
                    if len(self.lang_d2[l]) > 20:
                        self.lang_d2[l] = ['*']
                    elif self.lang_d2[l][0] != '*':
                        self.lang_d2[l].append(key)
                else:
                    self.lang_d2[l] = [key]
            p = self.__rex(r'<img [^<>]*?src="p/([^<>]+?)"[^<>]*>', re.I)
            for m in p.finditer(line):
                f = m.group(1)
                if not f in self.img_d:
                    self.img_d[f] = ''.join(['<tr><td>', key, '</td><td>', f, '</td><td>', m.group(0), '</td></tr>'])
        line = ''.join(['<link rel="stylesheet"href="', self.DIC_T, '.css"type="text/css"><div class="wcv">', line, '</div>'])
        if make_entry:
            line = '\n'.join([key, line, '</>'])
        return line

    def __not_eq_key(self, k1, k2):
        return k1.lower()!=k2.lower()

    def __mk_js(self, limit):
        if limit:
            dimn = int(limit.split('=')[1])
            if dimn > 495:
                return ' onclick="{if(this.className==\'q3j\')this.className=\'igf\';else this.className=\'q3j\'}"'
        return ''

    def __insert_illu(self, p, key, sup, line, illu):
        if len(illu[key][sup]) > 1:
            img = ''.join([''.join(['<img src="p/', f[1], '" class="q3j"', f[2], self.__mk_js(f[2]), '>']) for f in sorted(illu[key][sup], key=lambda t: t[0])])
        else:
            f = illu[key][sup][0]
            img = ''.join(['<img src="p/', f[1], '" class="q3j"', f[2], self.__mk_js(f[2]), '>'])
        line, n = p.subn(''.join([r'\1', img]), line, 1)
        return line, n

    def __new_sk(self, ls, key, sk, spl=' '):
        kp = key.strip(spl).split(spl)
        lk = len(kp)
        if ls < lk:
            pre = spl.join(kp[:lk-ls])
            if self.__not_eq_key(pre, sk) and not sk.lower().startswith(pre):
                sk = ''.join([pre, spl, sk])
                self.__logs.append('I07:\tlink generated\t%s\t->%s'%(sk, key))
        return sk

    def refine(self, key, line, illu, phvs):
        if _DEBUG_:
            p = self.__rex(r'.{5,15}</?(?:font|SMALL|NOBR|BR/?)[^<>]*>', re.I)
            m = p.search(line)
            if m:
                self.__logs.append('E0F:\tword not formated\t%s\t%s'%(key, m.group(0)))
        if key in illu:
            clear = 1
            for sup in illu[key]:
                if sup:
                    p = self.__rex(''.join([r'(</span>\s*<sup>', sup, '</sup></div>)']), re.I)
                else:
                    p = self.__rex(r'(<div class="nek">.+?</div>)', re.I)
                line, n = self.__insert_illu(p, key, sup, line, illu)
                clear = clear and n
            if clear:
                del illu[key]
        p = self.__rex(r'<div class="nek">(.+?)</div>', re.I)
        q = self.__rex(r'<span class="upo">(.+?)</span>', re.I)
        first = True
        for tt in p.findall(line):
            vars = q.findall(tt)
            for var in vars:
                sk = self.__mk_sk(var)
                if first:
                    first = False
                    if key.startswith(sk.lower()):
                        continue
                if self.__not_eq_key(sk, key):
                    if self.__not_eq_key(self.__rex(r'\W').sub('', sk), self.__rex(r'\W').sub('', key)):
                        skp = sk.replace('-', ' ').split(' ')
                        sk = self.__new_sk(len(skp), key, sk)
                    if sk.lower() in illu and '' in illu[sk.lower()]:
                        rv = self.__rex(r'([\(\)\.\[\]\*\+\?\|])').sub(r'\\\1', var)
                        p = re.compile(''.join([r'(', rv, r'</span></div>)']), re.I)
                        line, n = self.__insert_illu(p, sk.lower(), '', line, illu)
                        if n:
                            del illu[sk.lower()]
                    phvs.append((sk, ''.join(['@@@LINK=', key])))
        p = self.__rex(r'<div class="(?:duv|ihl|l1v|yjs)">(.+?)</div>', re.I)
        q = self.__rex(r'<span class="upo">(.+?)(?=</span>)', re.I)
        for f in p.findall(line):
            for var in q.findall(f):
                sk = self.__mk_sk(var)
                if self.__not_eq_key(sk, key):
                    phvs.append((sk, ''.join(['@@@LINK=', key])))
        p = self.__rex(r'(?:<div class="dlq"><div class="xmk">|<div class="duy">)<span class="upo">(.+?)(?=</span>)|<span class="gpk">(.+?)(?=</span>)', re.I)
        for d1, d2 in p.findall(line):
            drv = d1 if d1 else d2
            skl = self.__mk_sk(drv)
            for sk in skl.split(';'):
                sk = sk.strip()
                if sk and self.__not_eq_key(sk, key):
                    phvs.append((sk, ''.join(['@@@LINK=', key])))
        p = self.__rex(r'(<div class="j5c">.+?</div>)@(?=</div>)', re.I)
        line = p.sub(r'\1', line)
        p = self.__rex(r'<div class="a4p">(<div class="xmk">.+?</div>)(<div class="j5c">.+?</div>)#</div>', re.I)
        line =p.sub(lambda m: self.__regphv(m, key, phvs), line)
        p = self.__rex(r'(?<!</p>)<p(><a href=[^<>]+>[^<>]+</a></)p>(?!<p\b)', re.I)
        line = p.sub(r'<div class="s3s"\1div>', line)
        p = self.__rex(r'(?<!</p>)(?=<p><a href=[^<>]+>[^<>]+</a></p>)', re.I)
        line = p.sub(r'<div class="sms"></div>', line)
        p = self.__rex(r'(<p><a href=[^<>]+>[^<>]+</a></p>)(?!<p\b)', re.I)
        line = p.sub(r'\1<div class="sms"></div>', line)
        return '\n'.join([key, line, '</>\n'])

    def uni_phvs(self, phvs, entries, dir):
        lns = OrderedDict()
        p = self.__rex(r'<div class="j5c">(?:<span class="gsh">|<a [^<>]+>)\s*See\b', re.I)
        q = self.__rex(r'(?<=<span class="thw">)(.+?)(?=</span>)', re.I)
        a = self.__rex(r'<a[^<>]+>[^<>]+</a>', re.I)
        for word, ent in phvs:
            sk = word.lower()
            if sk in entries:
                if ent.startswith('@@@'):
                    self.__logs.append('I04:\tignore link %s -> %s'%(word, ent))
                    continue
                else:
                    self.__logs.append('I05:\tdulplicate phvs, check %s'%word)
            if sk in lns:
                if ent.startswith('@@@'):
                    self.__logs.append('I04:\tignore link %s => %s'%(word, ent))
                else:
                    oe = lns[sk]
                    if oe.startswith('@@@'):
                        lns[sk] = '\n'.join([word, ent, '</>\n'])
                        self.__logs.append('I04:\tignore link %s >> %s'%(word, oe))
                    elif p.search(ent):
                        ig, ent = ent, oe
                    elif p.search(oe):
                        ig, ent = oe.replace('\n', ''), '\n'.join([word, ent, '</>\n'])
                    else:
                        ig = None
                    if ig:
                        prt = ', '.join(a.findall(q.search(ig).group(1)))
                        lns[sk] = q.sub(''.join([r'\1, ', prt]), ent)
                        self.__logs.append('I06:\tignore phv %s=%s'%(word, ig))
                    else:
                        lns[sk] = ''.join([oe, word, '\n', ent, '\n</>\n'])
                        self.__logs.append('I08:\tcombine dulplicate phvs, check %s'%word)
            else:
                lns[sk] = '\n'.join([word, ent, '</>\n'])
        dump('\n'.join(lns.keys()), ''.join([dir, 'phrases.txt']))
        return lns.values()


def is_complete(dir, ext='.part'):
    if path.exists(dir):
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(ext):
                    return False
        return True
    return False


if __name__=="__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    import argparse
    argpsr = argparse.ArgumentParser()
    argpsr.add_argument("diff", nargs="?", help="[f] format only")
    argpsr.add_argument("file", nargs="?", help="[file name] To specify additional wordlist when diff is [p]")
    args = argpsr.parse_args()
    print "Start at %s" % datetime.now()
    dic_dl = wbd_downloader()
    dir = ''.join([dic_dl.DIC_T, path.sep])
    if args.diff == 'f':
        if is_complete(fullpath(dir)):
            dic_dl.combinefiles(dir)
        else:
            print "Word-downloading is not completed."
    else:
        dic_dl.login()
        if dic_dl.session:
            d_all, base = getwordlist('wordlist.txt'), 0
            print len(d_all)
            if args.diff=='p' and args.file and path.exists(fullpath(args.file)):
                print "Start to download missing words..."
                d_p, wordlist = getwordlist(args.file), []
                for d in os.listdir(fullpath(dir)):
                    if re.compile(r'^\d+$').search(d) and path.isdir(fullpath(''.join([dir, d, path.sep]))):
                        base += 1
                for i in xrange(1, base+1):
                    sdir = ''.join([dir, '%d'%i, path.sep])
                    if path.exists(fullpath('appd.txt', base_dir=sdir)):
                        d_p.update(getwordlist(''.join([sdir, 'appd.txt'])))
                for w in d_p.keys():
                    if w in d_all:
                        del d_p[w]
                    else:
                        wordlist.append(w)
                d_all.update(d_p)
            else:
                wordlist, d_p = d_all.keys(), OrderedDict()
            while wordlist:
                blks, addlist = multiprocess_fetcher(dir, d_all, wordlist, dic_dl, base)
                base += blks
                wordlist = addlist
                d_p = OrderedDict([(k, None) for k in addlist])
                if addlist:
                    print "Downloading additional words..."
                    d_all.update(d_p)
            if is_complete(fullpath(dir)):
                dic_dl.combinefiles(dir)
            print "Done!"
        else:
            print "ERROR: Login failed."
    print "Finished at %s" % datetime.now()
