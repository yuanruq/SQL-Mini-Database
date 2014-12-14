/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingRootDocWrapper extends HidingWrapper implements RootDoc {
    public HidingRootDocWrapper(RootDoc rootdoc, Map mapWrappers) {
        super(rootdoc, mapWrappers);
    }

    private RootDoc _getRootDoc() {
        return (RootDoc)getWrappedObject();
    }

    public ClassDoc classNamed(String szName) {
        return (ClassDoc)wrapOrHide(_getRootDoc().classNamed(szName));
    }

    public ClassDoc[] classes() {
        return (ClassDoc[])wrapOrHide(_getRootDoc().classes());
    }

    public String[][] options() {
        return _getRootDoc().options();
    }

    public PackageDoc packageNamed(String szName) {
        return (PackageDoc)wrapOrHide(_getRootDoc().packageNamed(szName));
    }

    public ClassDoc[] specifiedClasses() {
        return (ClassDoc[])wrapOrHide(_getRootDoc().specifiedClasses()); 
    }

    public PackageDoc[] specifiedPackages() {
        return (PackageDoc[])wrapOrHide(_getRootDoc().specifiedPackages());
    }

    public void printError(String szError) {
        _getRootDoc().printError(szError);
    }

    public void printError(SourcePosition pos, String szError) {
        _getRootDoc().printError(pos, szError);
    }

    public void printWarning(String szWarning) {
        _getRootDoc().printWarning(szWarning);
    }

    public void printWarning(SourcePosition pos, String szWarning) {
        _getRootDoc().printWarning(pos, szWarning);
    }

    public void printNotice(String szNotice) {
        _getRootDoc().printNotice(szNotice);
    }

    public void printNotice(SourcePosition pos, String szNotice) {
        _getRootDoc().printNotice(pos, szNotice);
    }

    public boolean isAnnotationType() {
        return _getRootDoc().isAnnotationType();
    }

    public boolean isEnum() {
        return _getRootDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getRootDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getRootDoc().isEnumConstant();
    }
  
    public String commentText() {
        return _getRootDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getRootDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getRootDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getRootDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getRootDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getRootDoc().inlineTags());
    }

    public boolean isClass() {
        return _getRootDoc().isClass();
    }

    public boolean isConstructor() {
        return _getRootDoc().isConstructor();
    }

    public boolean isError() {
        return _getRootDoc().isError();
    }

    public boolean isException() {
        return _getRootDoc().isException();
    }

    public boolean isField() {
        return _getRootDoc().isField();
    }

    public boolean isIncluded() {
        return _getRootDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getRootDoc().isInterface();
    }

    public boolean isMethod() {
        return _getRootDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getRootDoc().isOrdinaryClass();
    }

    public String name() {
        return _getRootDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getRootDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getRootDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getRootDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getRootDoc().tags(szTagName));
    }
   
    public SourcePosition position() {
        return _getRootDoc().position();
    }
}
